import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";

@Injectable({ providedIn: 'root' })
export class TournamentService {
  constructor(
    private readonly _http: HttpClient
  ) {
  }

  public loadBrackets(): Observable<any> {
    const url = `http://localhost:3000/fetch-tournaments`;
    return this._http.get<any[]>(url);
  }
}