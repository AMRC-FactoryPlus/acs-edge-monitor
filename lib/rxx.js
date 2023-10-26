/* 
 * ACS Edge Monitor service
 * Rx operators
 * Copyright 2023 AMRC
 */

import rx from "rxjs";

export function catchType (errclass, handler) {
    return rx.catchError(err => {
        if (!(err instanceof errclass))
            throw err;
        return handler(err);
    });
}
