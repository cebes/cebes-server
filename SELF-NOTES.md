- Handling exceptions: http://doc.akka.io/docs/akka/2.4.9/scala/http/routing-dsl/exception-handling.html
- Use gRPC to do REST with JSON: http://www.grpc.io/blog/coreos
  but this requires [gRPC REST Gateway](https://github.com/gengo/grpc-gateway), which is in Go only.
  Apparently we can do similar like this: https://groups.google.com/forum/#!topic/grpc-io/8jhVAttQBC4
  but that is a bit cumbersome.
  So the best way is to wait until grpc-gateway is available in Java/Scala (if that happens at all...)

- Pipeline design:
    - Pipelines are not thread-safe! Every time you create a new pipeline, it is a new object (with a new ID),
    and every changes you made only effect that particular pipeline object. When you tag a pipeline, then load
    it back using the tag, a new ID is given to the returned pipeline.

    - Usually this doesn't have any effect to the end users, because they will mostly work with pipelines using
    the `with` statement. However, pipeline objects are not meant to be passed between different threads and
    execute concurrently. If you really need to do that, tag the pipeline, and load it back in each thread
    separately. In this case, the pipeline objects are separated.

- Jenkins Security Policy:

    Go to cebes.io:8080/script, then run this:

        System.getProperty("hudson.model.DirectoryBrowserSupport.CSP")
        System.setProperty("hudson.model.DirectoryBrowserSupport.CSP", "img-src 'self' data:; style-src 'self' 'unsafe-inline' https://netdna.bootstrapcdn.com https://cdnjs.cloudflare.com; child-src 'self'; frame-src 'self' 'unsafe-inline'")
        System.getProperty("hudson.model.DirectoryBrowserSupport.CSP")

- Long-term plan:
  - Scala library
  - Pipeline editor (Azure-like)