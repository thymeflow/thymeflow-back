package com.thymeflow

import com.thymeflow.utilities.Result

/**
  * @author Thomas Pellissier Tanon
  */
package object update {
  type UpdateResult = Result[Unit, List[Exception]]
}
