<?php
/*
 * Copyright (C) 2019 Thamtech, LLC
 *
 * This software is copyrighted. No part of this work may be
 * reproduced in whole or in part in any manner without the
 * permission of the Copyright owner, unless specifically authorized
 * by a license obtained from the Copyright owner.
**/

namespace thamtech\fluentd;

/**
 * FluentClientInterface defines an interface for clients attached to a
 * FluentTarget log target.
 *
 * @author Tyler Ham <tyler@thamtech.com>
 */
interface FluentClientInterface
{
    /**
     * Emit a single record
     *
     * @param  string $tag The Fluentd tag
     * @param  mixed $record A serializable record to send
     * @param  float|int $timestamp The time in seconds
     *
     * @return bool true if successful
     */
    public function emit($tag, $record, $timestamp = null);

    /**
     * Emit multiple records
     *
     * @param  string $tag The Fluentd tag
     * @param  array $records An array of serializable records to send
     * @param  float|int $timestamp The time in seconds
     *
     * @return bool[] an array indicating the success or failure of each record
     */
    public function emitBatch($tag, array $records, $timestamp = null);
}
