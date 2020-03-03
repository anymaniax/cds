import {
    ChangeDetectionStrategy,
    ChangeDetectorRef,
    Component,
    OnInit,
    ViewChild
} from "@angular/core";
import { TranslateService } from "@ngx-translate/core";
import { Store } from "@ngxs/store";
import { AuthentifiedUser } from "app/model/user.model";
import { ConfigService } from "app/service/config/config.service";
import { ThemeStore } from "app/service/theme/theme.store";
import { PathItem } from "app/shared/breadcrumb/breadcrumb.component";
import { AutoUnsubscribe } from "app/shared/decorator/autoUnsubscribe";
import { AuthenticationState } from "app/store/authentication.state";
import { Subscription } from "rxjs";
import { finalize } from "rxjs/operators";
import { CDSCTL_ARCH, CDSCTL_OS } from "./cdsctl.constants";

@Component({
    selector: "app-cdsctl",
    templateUrl: "./cdsctl.html",
    styleUrls: ["./cdsctl.scss"],
    changeDetection: ChangeDetectionStrategy.OnPush
})
@AutoUnsubscribe()
export class CdsctlComponent implements OnInit {
    @ViewChild("codemirror1", { static: false }) codemirror1: any;
    @ViewChild("codemirror2", { static: false }) codemirror2: any;
    @ViewChild("codemirror3", { static: false }) codemirror3: any;
    @ViewChild("codemirror4", { static: false }) codemirror4: any;
    @ViewChild("codemirror5", { static: false }) codemirror5: any;
    @ViewChild("codemirror6", { static: false }) codemirror6: any;
    @ViewChild("codemirror7", { static: false }) codemirror7: any;
    @ViewChild("codemirror8", { static: false }) codemirror8: any;

    currentUser: AuthentifiedUser;
    apiURL: string;
    arch: Array<string>;
    os: Array<string>;
    withKeychain: boolean;
    path: Array<PathItem>;
    codeMirrorConfig: any;
    tutorials: Array<string> = new Array();
    osChoice: string;
    archChoice: string;
    loading: boolean;
    themeSubscription: Subscription;

    constructor(
        private _store: Store,
        private _configService: ConfigService,
        private _translate: TranslateService,
        private _theme: ThemeStore,
        private _cd: ChangeDetectorRef
    ) {
        this.codeMirrorConfig = {
            matchBrackets: true,
            autoCloseBrackets: true,
            mode: "shell",
            lineWrapping: true,
            autoRefresh: true,
            readOnly: true,
            lineNumbers: true
        };

        this.withKeychain = true;
        this.os = new Array<string>(
            CDSCTL_OS.WINDOWS,
            CDSCTL_OS.LINUX,
            CDSCTL_OS.DARWIN,
            CDSCTL_OS.FREEBSD
        );
        this.arch = new Array<string>(
            CDSCTL_ARCH.AMD64,
            CDSCTL_ARCH.I386,
            CDSCTL_ARCH.ARM,
            CDSCTL_ARCH.ARM64
        );
        this.osChoice = CDSCTL_OS.LINUX;
        this.archChoice = CDSCTL_ARCH.AMD64;
    }

    ngOnInit() {
        this.themeSubscription = this._theme.get().subscribe(t => {
            this.codeMirrorConfig.theme = t === "night" ? "darcula" : "default";
            if (this.codemirror1 && this.codemirror1.instance) {
                this.codemirror1.instance.setOption(
                    "theme",
                    this.codeMirrorConfig.theme
                );
            }
            if (this.codemirror2 && this.codemirror2.instance) {
                this.codemirror2.instance.setOption(
                    "theme",
                    this.codeMirrorConfig.theme
                );
            }
            if (this.codemirror3 && this.codemirror3.instance) {
                this.codemirror3.instance.setOption(
                    "theme",
                    this.codeMirrorConfig.theme
                );
            }
            if (this.codemirror4 && this.codemirror4.instance) {
                this.codemirror4.instance.setOption(
                    "theme",
                    this.codeMirrorConfig.theme
                );
            }
            if (this.codemirror5 && this.codemirror5.instance) {
                this.codemirror5.instance.setOption(
                    "theme",
                    this.codeMirrorConfig.theme
                );
            }
            if (this.codemirror6 && this.codemirror6.instance) {
                this.codemirror6.instance.setOption(
                    "theme",
                    this.codeMirrorConfig.theme
                );
            }
            if (this.codemirror7 && this.codemirror7.instance) {
                this.codemirror7.instance.setOption(
                    "theme",
                    this.codeMirrorConfig.theme
                );
            }
            if (this.codemirror8 && this.codemirror8.instance) {
                this.codemirror8.instance.setOption(
                    "theme",
                    this.codeMirrorConfig.theme
                );
            }
            this._cd.markForCheck();
        });

        this.currentUser = this._store.selectSnapshot(AuthenticationState.user);

        this.loading = true;
        this._configService
            .getConfig()
            .pipe(
                finalize(() => {
                    this.loading = false;
                    this._cd.markForCheck();
                })
            )
            .subscribe(r => {
                this.apiURL = r["url.api"];
                this.loading = false;
                this.buildData();
            });
    }

    buildData(): void {
        let variant = "";
        if (!this.withKeychain) {
            variant = "?variant=nokeychain";
        }
        this.tutorials["part1"] = this._translate.instant(
            this.osChoice === CDSCTL_OS.WINDOWS
                ? "cdsctl_part_1_windows"
                : "cdsctl_part_1",
            {
                apiURL: this.apiURL,
                osChoice: this.osChoice,
                archChoice: this.archChoice,
                variant: variant
            }
        );
        this.tutorials["part2"] = this._translate.instant("cdsctl_part_2", {
            apiURL: this.apiURL,
            username: this.currentUser.username
        });
        this.tutorials["part3"] = this._translate.instant("cdsctl_part_3");
        this.tutorials["part4"] = this._translate.instant("cdsctl_part_4");
        this.tutorials["part5"] = this._translate.instant("cdsctl_part_5");
        this.tutorials["part6"] = this._translate.instant("cdsctl_part_6");
        this.tutorials["part7"] = this._translate.instant("cdsctl_part_7");
        this.tutorials["part8"] = this._translate.instant("cdsctl_part_8");

        this.path = [
            <PathItem>{
                translate: "common_settings"
            },
            <PathItem>{
                translate: "navbar_cdsctl",
                routerLink: ["/", "settings", "cdsctl"]
            }
        ];
    }
}
