import { AfterViewInit, Component, Input } from '@angular/core';
import { BracketsManager } from 'brackets-manager';
import { InMemoryDatabase } from 'brackets-memory-db';
import dataset from '../../data';
// import dataset, { katas } from '../../data';

import { StageViewerComponent } from './stage-viewer/stage-viewer.component';
import { TournamentService } from './tournament.service';

function getNearestPowerOfTwo(input: number): number {
  return Math.pow(2, Math.ceil(Math.log2(input)));
}

@Component({
  selector: 'app-tournament-page',
  standalone: true,
  imports: [StageViewerComponent],
  templateUrl: './tournament-page.component.html',
  styleUrl: './tournament-page.component.scss'
})
export class TournamentPageComponent implements AfterViewInit {
  private db: InMemoryDatabase = new InMemoryDatabase();
  private manager: BracketsManager;
  public dataset: any = dataset.filter(x => x.roster.length > 1);
  public emptyBrackets: any = dataset.filter(x => x.roster.length === 0);
  public smallBrackets: any = dataset.filter(x => x.roster.length <= 4 && x.roster.length === 1);
  public allDojos = new Set<string>(this.dataset.map((x: any) => x.roster.map((y: any) => this.getDojoName(y.name))).flat(Infinity));
  // public katas = katas;
  @Input('tournamentId') public tournamentId: string = 'tournament-1';
  public loadedData: any[] = [];

  public constructor(
    private readonly _tournamentService: TournamentService,
  ) {
    this.db = new InMemoryDatabase();
    this.manager = new BracketsManager(this.db);
  }

  private async loadBrackets(dataset: any): Promise<any> {
    const participants = dataset.map((item: any) => item.roster).flat(Infinity)
    console.log('Loading brackets for dataset:', participants);
    this.db.setData({
      participant: participants.map((player: any) => ({
        ...player,
        tournament_id: this.tournamentId,
      })),
      stage: [],
      group: [],
      round: [],
      match: [],
      match_game: [],
    });
    console.log('Database initialized with participants:');

    for (let ds of dataset) {
      await this.manager.create.stage({
        name: ds.title,
        tournamentId: this.tournamentId,
        type: ds.type,
        seeding: ds.roster.map((player: any) => `${player.name} [${player.weight} kg]`),
        settings: {
          seedOrdering: [ds.seedingOrdering ?? 'natural'],
          balanceByes: true,
          size: getNearestPowerOfTwo(ds.roster.length),
        },
      });
    }

    const data = await this.manager.get.tournamentData(this.tournamentId)
    console.log('Tournament data loaded:', data);
    const allData = [];
    for (let i = 0; i < data.stage.length; i++) {
      const stageId = data.stage[i].id;
      const stageData = await this.manager.get.stageData(stageId);
      allData.push({
        stages: stageData.stage,
        matches: stageData.match,
        matchGames: stageData.match_game,
        participants: stageData.participant,
      });
    }

    console.log('All stages data:', allData);
    return allData;
  }

  async ngAfterViewInit(): Promise<void> {
    (window as any).bracketsViewer.addLocale('en', {
      common: {
        'group-name-winner-bracket': '{{stage.name}}',
        'group-name-loser-bracket': '{{stage.name}} - Repechage',
      },
      'origin-hint': {
        'winner-bracket': 'WB {{round}}.{{position}}',
        'winner-bracket-semi-final': 'WB Semi {{position}}',
        'winner-bracket-final': 'WB Final',
        'consolation-final': 'Semi {{position}}',
      },
    });

    this._tournamentService.loadBrackets().subscribe({
      next: async (data: any[]) => {
        console.log('Data loaded from service:', data);
        this.loadedData = await this.loadBrackets(data.filter(x => x.roster.length > 1));
      },
      error: (error) => {
        console.error('Error loading data:', error);
      }
    });
    // this.loadedData = await this.loadBrackets(this.dataset);
  }

  public getDojoName(name: string): string {
    try {
      const dojoMatch = name.match(/\[(.*?)\]/);
      return dojoMatch ? dojoMatch[1] : '';
    } catch (error) {
      console.error('Error extracting dojo name:', error);
      return '';
    }
  }


}
