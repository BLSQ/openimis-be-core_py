import datetime

from django.db import transaction

from core.models import SpimmIDGenerator


@transaction.atomic
def fetch_next_value(hf_id, requested_field):
    if not hasattr(SpimmIDGenerator, requested_field):
        raise ValueError(f"the ID generator doesn't have any '{requested_field}' field")

    current_year = datetime.date.today().year
    try:
        id_generator = SpimmIDGenerator.objects.select_for_update().get(hf_id=hf_id)
    except SpimmIDGenerator.DoesNotExist:
        # If the object doesn't exist, create it with an initial value
        id_generator = SpimmIDGenerator.objects.create(
            hf_id=hf_id,
            next_insuree_id=1,
            next_claim_id=1,
            current_year=current_year,
        )

    if id_generator.current_year != current_year:
        id_generator.reset_for_current_year(current_year)

    searched_value = getattr(id_generator, requested_field)
    setattr(id_generator, requested_field, searched_value + 1)
    id_generator.save()

    return searched_value


@transaction.atomic
def fetch_next_insuree_id(hf_id):
    return fetch_next_value(hf_id, "next_insuree_id")


@transaction.atomic
def fetch_next_claim_id(hf_id):
    return fetch_next_value(hf_id, "next_claim_id")
