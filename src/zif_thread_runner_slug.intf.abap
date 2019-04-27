interface ZIF_THREAD_RUNNER_SLUG
  public .

  methods:
    run,
    serialize_params
      returning
        value(rv_xstr) type xstring,
    deserialize_params
      importing
        iv_xstr type xstring,
    serialize_result
      returning
        value(rv_xstr) type xstring,
    deserialize_result
      importing
        iv_xstr type xstring.

endinterface.
