# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'phosphoro_com'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Phosphoro_PySpider_italy"
    start_urls = ['https://www.phosphoro.com/lista/annunci/casa/short_casa/1/?filter=apply&home_affitti_search=true&dove=&durata=360&alloggio=&prezzo=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get('page', 2)

        seen = False
        for item in response.xpath("//a[@class='photo_link']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True

        if page == 2 or seen:
            url = f"https://www.phosphoro.com/lista/annunci/casa/short_casa/{page}/?filter=apply&home_affitti_search=true&dove=&durata=360&alloggio=&prezzo="
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        if get_p_type_string(response.url):
            item_loader.add_value(
                "property_type", get_p_type_string(response.url))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath(
            "//span[contains(@class,'mtitle')]//span[contains(@id,'id_value')]//text()").get()
        if external_id:
            item_loader.add_value(
                "external_id", external_id.split("dettagli annuncio ")[0])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title.replace("\u20ac",""))

        available_date = response.xpath("//div[@class='row_periodo']//span[contains(@class,'standard_detail_value')]//text()[contains(.,'/')]").get()
        if available_date:
            item_loader.add_value("available_date", available_date)

        address = response.xpath(
            "//div[@class='detail_row zona indirizzo_casa']//text()").getall()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath(
            "//div[@class='detail_row zona indirizzo_casa']//span[@class='capitalize city']//text()").get()
        if city:
            item_loader.add_value("city", city)

        square_meters = response.xpath(
            "//span[@class='label_value']//span[@class='standard_detail_value']//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath(
            "//h1[contains(@class,'detail_stanza')]//text()[contains(.,'€')]").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1].split("(")[0])
        else:
            rent = response.xpath(
                "//h1[contains(@class,'titolo_annuncio')]//text()[contains(.,'€')]").get()
            if rent:
                item_loader.add_value("rent", rent.split("€")[1].split("(")[0])
        item_loader.add_value("currency", "EUR")

        room_count = response.xpath("//div[@class='row_description_stanza tipo_doppia']//text()[contains(.,'posto')]").get()
        if room_count:
            room_count="".join(room_count.split(" ")[-2:-1])
            item_loader.add_value("room_count", room_count)

        description = response.xpath(
            "//div[contains(@class,'text_box_note_dettagli')]//following-sibling::i//text()").getall()
        if description:
            item_loader.add_value("description", description)
        else:
            description = response.xpath(
                "//div[contains(@class,'text_box_note_dettagli')]/text()").getall()
            if description:
                item_loader.add_value("description", description)

        furnished = response.xpath(
            "//span[contains(@class,'standard_detail_value')]//text()[contains(.,'arredato')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        terrace = response.xpath(
            "//span[contains(@class,'label_value')]//text()[contains(.,'terrazzi - balconi')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)

        balcony = response.xpath(
            "//span[contains(@class,'label_value')]//text()[contains(.,'terrazzi - balconi')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)


        images = [response.urljoin(x) for x in response.xpath("//div[@class='single-photo']//a//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)


        item_loader.add_value("landlord_phone", "0461261200")
        item_loader.add_value("landlord_email", "trovacasa+00@phosphoro.com")
        item_loader.add_value("landlord_name", "phosphoro")
            
        status = response.xpath("//div[@class='lingua_box']//a//@href").get()     
        if status and "ufficio" not in status.lower() :
            yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "condiviso" in p_type_string.lower():
        return "room"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartament" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("casa" in p_type_string.lower() or "villetta" in p_type_string.lower()):
        return "house"
    else:
        return None
