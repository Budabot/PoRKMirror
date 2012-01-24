package com.jkbff.ao.playerinfoscraper

object Helper {
	def using[T <: { def close(): Unit }](resource: T)(op: T => Unit) {
		try {
			op(resource)
		} finally {
			resource.close()
		}
	}
}