class MyAwesomeModel(clearskies.Model):
    id_column_name = "id"
    backend = clearskies.backends.MemoryBackend()

    id = columns.Uuid()
    name = clearskies.columns.String()
    email = columns.Email(validators=[validators.Unique()])
    created_at = columns.Created()
