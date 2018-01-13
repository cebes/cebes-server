package io.cebes.tag

import org.scalatest.FunSuite

/**
  * Created by d066177 on 01/01/2017.
  */
class TagSuite extends FunSuite {

  test("simple cases") {
    val tag1 = Tag.fromString("simple-tag_ab.c")
    assert(tag1.name === "simple-tag_ab.c")
    assert(tag1.host.isEmpty)
    assert(tag1.port.isEmpty)
    assert(tag1.server.isEmpty)
    assert(tag1.path === Some("simple-tag_ab.c"))
    assert(tag1.version === "default")
    assert(tag1.toString === "simple-tag_ab.c:default")

    val ex1 = intercept[IllegalArgumentException] {
      Tag.fromString("simple-tag_ab.cTA")
    }
    assert(ex1.getMessage.startsWith("Invalid tag expression"))

    // tag with version
    val tag2 = Tag.fromString("simple-tag:v1")
    assert(tag2.name === "simple-tag")
    assert(tag2.host.isEmpty)
    assert(tag2.port.isEmpty)
    assert(tag2.server.isEmpty)
    assert(tag2.path === Some("simple-tag"))
    assert(tag2.version === "v1")
    assert(tag2.toString === "simple-tag:v1")

    // hostname doesn't contain a dot
    intercept[IllegalArgumentException] {
      Tag.fromString("simple-tag:9000/abc-sz")
    }

    // with path and host
    val tag3 = Tag.fromString("simple-tag.com:9000/abc-sz")
    assert(tag3.name === "simple-tag.com:9000/abc-sz")
    assert(tag3.host === Some("simple-tag.com"))
    assert(tag3.port === Some(9000))
    assert(tag3.server === Some("simple-tag.com:9000/"))
    assert(tag3.path === Some("abc-sz"))
    assert(tag3.version === "default")
    assert(tag3.toString === "simple-tag.com:9000/abc-sz:default")

    // with path and host and version
    val tag4 = Tag.fromString("simple-tag.com:9000/abc-sz:default")
    assert(tag4.name === "simple-tag.com:9000/abc-sz")
    assert(tag4.host === Some("simple-tag.com"))
    assert(tag4.port === Some(9000))
    assert(tag4.server === Some("simple-tag.com:9000/"))
    assert(tag4.path === Some("abc-sz"))
    assert(tag4.version === "default")
    assert(tag4.toString === "simple-tag.com:9000/abc-sz:default")

    // fuzzy case
    val tag5 = Tag.fromString("simple-tag:9000")
    assert(tag5.name === "simple-tag")
    assert(tag5.host.isEmpty)
    assert(tag5.port.isEmpty)
    assert(tag5.server.isEmpty)
    assert(tag5.path === Some("simple-tag"))
    assert(tag5.version === "9000")
    assert(tag5.toString === "simple-tag:9000")

    // another fuzzy case
    intercept[IllegalArgumentException] {
      Tag.fromString("simple-tag.com:9000:200")
    }
    val tag6 = Tag.fromString("simple-tag.com:9000/a:200")
    assert(tag6.name === "simple-tag.com:9000/a")
    assert(tag6.host === Some("simple-tag.com"))
    assert(tag6.port === Some(9000))
    assert(tag6.server === Some("simple-tag.com:9000/"))
    assert(tag6.path === Some("a"))
    assert(tag6.version === "200")
    assert(tag6.toString === "simple-tag.com:9000/a:200")

    val tag7 = Tag.fromString("abc/def/ghi:v100")
    assert(tag7.name === "abc/def/ghi")
    assert(tag7.host.isEmpty)
    assert(tag7.port.isEmpty)
    assert(tag7.server.isEmpty)
    assert(tag7.path === Some("abc/def/ghi"))
    assert(tag7.version === "v100")
    assert(tag7.toString === "abc/def/ghi:v100")

    val ex3 = intercept[IllegalArgumentException] {
      Tag.fromString("simple-tag:500/:v2")
    }
    assert(ex3.getMessage.startsWith("Invalid tag expression"))

    val ex4 = intercept[IllegalArgumentException] {
      Tag.fromString("")
    }
    assert(ex4.getMessage.startsWith("Invalid tag expression"))
  }

  test("localhost") {
    val tag0 = Tag.fromString("localhost/abc")
    assert(tag0.host === Some("localhost"))
    assert(tag0.port.isEmpty)
    assert(tag0.path === Some("abc"))
    assert(tag0.version === "default")

    val tag1 = Tag.fromString("localhost/abc:v1")
    assert(tag1.host === Some("localhost"))
    assert(tag1.port.isEmpty)
    assert(tag1.path === Some("abc"))
    assert(tag1.version === "v1")

    val tag2 = Tag.fromString("localhost:22000/abc")
    assert(tag2.host === Some("localhost"))
    assert(tag2.port === Some(22000))
    assert(tag2.path === Some("abc"))
    assert(tag2.version === "default")

    val tag3 = Tag.fromString("localhost:22000/abc:v3")
    assert(tag3.host === Some("localhost"))
    assert(tag3.port === Some(22000))
    assert(tag3.path === Some("abc"))
    assert(tag3.version === "v3")

    val tag4 = Tag.fromString("notonlocalhost/abc")
    assert(tag4.host.isEmpty)
    assert(tag4.port.isEmpty)
    assert(tag4.path === Some("notonlocalhost/abc"))
    assert(tag4.version === "default")

    val tag5 = Tag.fromString("localhostblah/abc")
    assert(tag5.host.isEmpty)
    assert(tag5.port.isEmpty)
    assert(tag5.path === Some("localhostblah/abc"))
    assert(tag5.version === "default")

    intercept[IllegalArgumentException] {
      Tag.fromString("notonlocalhost:2000/abc")
    }
  }
}
