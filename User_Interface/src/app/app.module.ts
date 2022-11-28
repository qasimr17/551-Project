import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import {MatSidenavModule} from '@angular/material/sidenav';
import {MatToolbarModule} from '@angular/material/toolbar';
import {MatSelectModule} from '@angular/material/select';
import { FontAwesomeModule } from '@fortawesome/angular-fontawesome';
import {MatButtonModule} from '@angular/material/button';
import { AddFolderComponent } from './add-folder/add-folder.component'
import {MatDialogModule} from '@angular/material/dialog';
import { AddFileComponent } from './add-file/add-file.component'
import { MatTableModule } from '@angular/material/table';
import { SearchComponent } from './search/search.component';
import { FormsModule } from '@angular/forms';
import { ViewPartitionComponent } from './view-partition/view-partition.component';
import {HttpClientModule} from '@angular/common/http';
import {NgToastModule} from 'ng-angular-popup';
import { RemoveFileComponent } from './remove-file/remove-file.component';
import {NgxSpinnerModule} from 'ngx-spinner';

@NgModule({
  declarations: [
    AppComponent,
    AddFolderComponent,
    AddFileComponent,
    SearchComponent,
    ViewPartitionComponent,
    RemoveFileComponent,
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    MatSidenavModule,
    MatToolbarModule,
    MatSelectModule,
    FontAwesomeModule,
    MatButtonModule,
    MatDialogModule,
    MatTableModule,
    FormsModule,
    HttpClientModule,
    NgToastModule,
    NgxSpinnerModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
