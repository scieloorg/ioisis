from ioisis.java import generator_blocking_process, jvm


def test_jvm_get_datetime_multiple_calls():
    @generator_blocking_process
    def dt_gen(inc_days):
        with jvm():
            from java.time import LocalDateTime
            from java.time.format import DateTimeFormatter
            dt = LocalDateTime.of(2019, 11, 26, 16, 30, 0)
            dtfmt = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
            for inc in range(1, inc_days + 1):
                yield dt.plusDays(inc).format(dtfmt)

    # It should work more than once
    assert list(dt_gen(1)) == ["27/11/2019 16:30:00"]
    assert list(dt_gen(2)) == ["27/11/2019 16:30:00", "28/11/2019 16:30:00"]
    assert list(dt_gen(0)) == []
