import clearskies


class MyAwesomeModel(clearskies.Model):
    id_column_name = "id"
    backend = clearskies.backends.MemoryBackend()

    id = clearskies.columns.Uuid()
    name = clearskies.columns.String()
    email = clearskies.columns.Email(validators=[clearskies.validators.Unique()])
    created_at = clearskies.columns.Created()
