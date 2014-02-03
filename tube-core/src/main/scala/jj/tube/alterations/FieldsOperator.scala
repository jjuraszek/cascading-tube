package jj.tube.alterations

import cascading.tuple.Fields
import cascading.pipe.assembly.{Coerce, Retain, Rename, Discard}
import cascading.pipe.Each
import cascading.operation.Insert
import cascading.tuple.Fields._
import jj.tube.Tube

trait FieldsOperator {
  this: Tube =>

  /**
   * Discard fields
   *
   * @param field fields to remove from tube
   * @return original fields - removed fields
   */
  def discard(field: Fields) = this << new Discard(this, field)

  /**
   * Rename some fields
   * @param from fields to rename
   * @param to new names (quantity of fields name must be equal to 'from')
   * @return original fields - from + to
   */
  def rename(from: Fields, to: Fields) = this << new Rename(this, from, to)

  /**
   * Keep fields listed in {@code fields}
   *
   * @param fields
   * @return fields from param 'fields'
   */
  def retain(fields: Fields) = this << new Retain(this, fields)

  /**
   * Alter type of fields.
   * @param fields fields for transformation
   * @tparam T type of destination class
   * @return same as input fields. Only values may be altered (ie. Double -> Integer)
   */
  def coerce[T](fields:Fields)(implicit m: Manifest[T]) = this << new Coerce(this, fields, (1 to fields.size).map(_ => m.runtimeClass): _*)

  /**
   * Append some constances to tube
   *
   * @param field name of appended constances
   * @param value values for each new field
   * @return input fields + 'fields' from param
   */
  def insert(field: Fields, value: String*) = this << new Each(this, new Insert(field, value: _*), ALL)
}
