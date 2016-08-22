package com.thymeflow.spatial

/**
  * @author Thomas Pellissier Tanon
  */
case class SimpleAddress(houseNumber: Option[String] = None,
                         street: Option[String] = None,
                         locality: Option[String] = None,
                         postalCode: Option[String] = None,
                         region: Option[String] = None,
                         country: Option[String] = None) extends Address
