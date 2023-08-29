import os
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
from constants import vars
from src.cohort import Cohort, SelectionCriterion
from src.steps import (
    InputStep, LoadStep, 
    AggStep, FilterStep, TransformStep, CustomStep, DropStep, RenameStep,
    Pipeline
)
from src.ricu import stay_windows, hours
from src.ricu_utils import (
    stop_window_at, make_grid_mapper, make_patient_mapper,
    n_obs_per_row, longest_rle
)

from utils import make_argument_parser, output_yaib, output_clairvoyance
outc_var = "death_icu"


def create_mortality_task(args):
    print('Start creating the mortality task.')
    print('   Preload variables')
    load_mortality = LoadStep(outc_var, args.src, cache=True)
    load_static = LoadStep(vars.static_vars, args.src, cache=True)
    load_dynamic = LoadStep(vars.dynamic_vars, args.src, cache=True)

    print('   Define observation times')
    time_of_death = load_mortality.perform()
    time_of_death = time_of_death[time_of_death[outc_var] == True]
    
    patients = stay_windows(args.src)
    patients = stop_window_at(patients, end=24)
    patients = stop_window_at(patients, end=time_of_death)

    print('   Define exclusion criteria')
    # General exclusion criteria
    excl1 = SelectionCriterion('Invalid length of stay')
    excl1.add_step([
        InputStep(patients),
        FilterStep('end', lambda x: x < 0)
    ])

    excl2 = SelectionCriterion('Length of stay < 6h')
    excl2.add_step([
        LoadStep('los_icu', args.src),
        FilterStep('los_icu', lambda x: x < 6 / 24)
    ])

    excl3 = SelectionCriterion('Less than 4 hours with any measurement')
    excl3.add_step([
        load_dynamic,
        AggStep('stay_id', 'count'),
        FilterStep('time', lambda x: x < 4)
    ])

    excl4 = SelectionCriterion('More than 12 hour gap between measurements')
    excl4.add_step([
        load_dynamic, 
        CustomStep(make_grid_mapper(patients, step_size=1)),
        CustomStep(n_obs_per_row),
        TransformStep('n', lambda x: x > 0), 
        AggStep('stay_id', longest_rle, 'n'),
        FilterStep('n', lambda x: x > 12)
    ])

    excl5 = SelectionCriterion('Aged < 18 years')
    excl5.add_step([
        LoadStep('age', args.src),
        FilterStep('age', lambda x: x < 18)
    ])

    # Task-specific exclusion criteria
    excl6 = SelectionCriterion('Died within the first 30 hours of ICU admission')
    excl6.add_step([
        LoadStep('death_icu', src=args.src, interval=hours(1), cache=True),
        FilterStep('death_icu', lambda x: x == True),
        FilterStep('time', lambda x: x < 30)
    ])

    excl7 = SelectionCriterion('Length of stay < 30h')
    excl7.add_step([
        LoadStep('los_icu', src=args.src, cache=True),
        FilterStep('los_icu', lambda x: x < 30/24)
    ])

    print('   Select cohort\n')
    cohort = Cohort(patients)
    cohort.add_criterion([excl1, excl2, excl3, excl4, excl5, excl6, excl7])
    print(cohort.criteria)
    patients, attrition = cohort.select()
    print('\n')

    print('   Load and format input data')
    outc_formatting = Pipeline("Prepare mortality")
    outc_formatting.add_step([
        load_mortality, 
        DropStep('time'),
        CustomStep(make_patient_mapper(patients)),
        TransformStep(outc_var, lambda x: x.fillna(0).astype(int)),
        RenameStep(outc_var, 'label')
    ])
    outc = outc_formatting.apply()
    
    dyn_formatting = Pipeline("Prepare dynamic variables")
    dyn_formatting.add_step([
        load_dynamic,
        CustomStep(make_grid_mapper(patients, step_size=1))
    ])
    dyn = dyn_formatting.apply()

    sta_formatting = Pipeline("Prepare static variables")
    sta_formatting.add_step([
        load_static,
        CustomStep(make_patient_mapper(patients))
    ])
    sta = sta_formatting.apply()

    return (outc, dyn, sta), attrition


if __name__ == "__main__":
    parser = make_argument_parser()
    args = parser.parse_known_args()[0]

    (outc, dyn, sta), attrition = create_mortality_task(args)

    save_dir = os.path.join(args.out_dir, args.src)

    if args.out_type is "yaib":
        output_yaib(outc, dyn, sta, attrition, save_dir)
    elif args.out_type is "clairvoyance":
        output_clairvoyance(outc, dyn, sta, attrition, save_dir)
    else:
        raise ValueError("Unknown output type. Please implement it or choose from the supplied options.")
