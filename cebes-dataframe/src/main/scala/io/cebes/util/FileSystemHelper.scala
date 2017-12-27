/* Copyright 2016 The Cebes Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, version 2.0 (the "License").
 * You may not use this work except in compliance with the License,
 * which is available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */
package io.cebes.util

import java.io._
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}

import com.typesafe.scalalogging.LazyLogging


/**
  * Helper with functions to archive and extract files.
  */
trait FileSystemHelper extends LazyLogging {

  /**
    * Copy all bytes from inStream to outStream, using buffer of the given size (in bytes).
    *
    * @return the total size copied
    */
  def copyStream(inStream: InputStream, outStream: OutputStream, bufferSize: Int = 8192): Long = {
    val buffer = new Array[Byte](bufferSize)

    def copy(copiedSize: Long): Long = {
      val l = inStream.read(buffer)
      if (l < 0) {
        copiedSize
      } else {
        outStream.write(buffer, 0, l)
        copy(copiedSize + l)
      }
    }

    copy(0)
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Put all files in the input folder into a zip package, stored at outputFile.
    * There will be no zip entry for the top level folder.
    */
  def zipFolder(inputFolder: String, outputFile: String): Unit = {
    def safePathConcat(s1: String, s2: String): String = s1 match {
      case "" => s2
      case ss => new File(ss, s2).toString
    }

    def addFile(parentPath: String, f: File, zip: ZipOutputStream): Unit = {
      if (f.isFile) {
        // copy the file
        zip.putNextEntry(new ZipEntry(safePathConcat(parentPath, f.getName).toString))
        ResourceUtil.using(new BufferedInputStream(new FileInputStream(f))) { inStream =>
          copyStream(inStream, zip)
        }
        zip.closeEntry()
      }
      if (f.isDirectory) {
        zip.putNextEntry(new ZipEntry(safePathConcat(parentPath, f.getName) + "/"))
        Option(f.listFiles()).foreach { children =>
          children.foreach { f1 =>
            addFile(safePathConcat(parentPath, f.getName).toString, f1, zip)
          }
        }
      }
    }

    ResourceUtil.using(new ZipOutputStream(new FileOutputStream(outputFile))) { zip =>
      Option(new File(inputFolder).listFiles()).foreach { children =>
        children.foreach(f => addFile("", f, zip))
      }
    }
  }

  /**
    * Unzip the given package into the given output path
    *
    * NOTE: This is intended to use for files created by [[zipFolder]] only. Extracting arbitrary
    * zip package might expose security threats.
    */
  def unzip(inputFile: String, outputPath: String): Unit = {
    def readZip(zip: ZipInputStream) {
      Option(zip.getNextEntry) match {
        case None =>
        case Some(entry) =>
          if (entry.getName.trim.endsWith("/")) {
            // this entry is a directory
            Paths.get(outputPath, entry.getName).toFile.mkdir()
          } else {
            ResourceUtil.using(new BufferedOutputStream(new FileOutputStream(new File(outputPath, entry.getName)))) {
              out => copyStream(zip, out)
            }
          }
          readZip(zip)
      }
    }

    ResourceUtil.using(new ZipInputStream(new FileInputStream(inputFile))) { zip =>
      readZip(zip)
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Delete a file or a directory recursively
    *
    * @param file   folder to be deleted
    * @param silent whether to raise an exception when we are failed to delete a file
    */
  def deleteRecursively(file: File, silent: Boolean = false): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(f => deleteRecursively(f, silent))
    if (file.exists && !file.delete)
      if (silent) {
        logger.error(s"Failed to delete ${file.toString}")
      } else {
        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
      }
  }

  /**
    * Move a directory into another directory, then delete the source
    */
  def moveDirectory(source: File, dest: File): Unit = {
    if (source.isDirectory) {
      if (!Files.exists(dest.toPath)) {
        Files.createDirectories(dest.toPath)
      }
      source.listFiles().foreach(f => moveDirectory(f, new File(dest, f.getName)))
      Files.deleteIfExists(source.toPath)
    }
    if (source.isFile) {
      Files.move(source.toPath, dest.toPath, StandardCopyOption.REPLACE_EXISTING)
    }
  }
}

object FileSystemHelper extends FileSystemHelper
