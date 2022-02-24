import {DThouseRestConnection} from './src/restConnect'

export function TDRestConnection(connection = {}) {
  return new DThouseRestConnection(connection)
}
