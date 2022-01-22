# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'sognandocasa_com'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Sognandocasa_PySpider_italy"
    start_urls = [
        'https://www.sognandocasa.com/elenco_immobili_f.asp?rel=nofollow']  # LEVEL 1

    formdata = {
        "riferimento": "",
        "cod_istat": "",
        "idcau": "2",
        "idtip": "5",
        "a_prezzo": "",
        "da_mq": "",
        "nvani": "0",
        "nr_camereg": "0",
        "nr_servizi": "0",
    }

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "5", "57"
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "32", "55", "63"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                self.formdata["idtip"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    dont_filter=True,
                    formdata=self.formdata,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//figure/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//title//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("rif:")[1])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath(
            "//h2[@class='title-left']//text()[contains(.,'Appartamento')]").get()
        if address:
            item_loader.add_value("address", address)

        description = response.xpath(
            "//div[@class='detail-title']//following-sibling::p[1]//text()").getall()
        if description:
            item_loader.add_value("description", description)

        balcony = response.xpath(
            "//ul[@class='list-three-col list-features']//following-sibling::li//text()[contains(.,'Balcone')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        parking = response.xpath(
            "//ul[@class='list-three-col list-features']//following-sibling::li//text()[contains(.,'Box Auto:')]").get()
        if parking:
            if 'nessuno' in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        room_count = response.xpath(
            "//ul[@class='list-three-col list-features']//following-sibling::li//text()[contains(.,'Vani')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Vani")[0])

        utilities = response.xpath(
            "//ul[@class='list-three-col list-features']//following-sibling::li//text()[contains(.,'mensili:')]").get()
        if utilities:
            item_loader.add_value(
                "utilities", utilities.split(":")[1].split("€")[0])

        square_meters = response.xpath(
            "//ul[@class='list-three-col list-features']//following-sibling::li//text()[contains(.,'Superficie:')]").get()
        if square_meters:
            item_loader.add_value(
                "square_meters", square_meters.split("Mq.")[1])

        energy_label = response.xpath(
            "//ul[@class='list-unstyled']//li//strong[contains(.,'Classe Energetica:')]//following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        rent = response.xpath(
            "//div[@class='detail-title']//following-sibling::span//text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1])
        item_loader.add_value("currency", "EUR")

        latitude_longitude = response.xpath(
            "//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                "google.maps.LatLng('")[1].split(",")[0]
            longitude = latitude_longitude.split(
                "google.maps.LatLng('")[1].split(",")[1].split("')")[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='slide']//a[contains(@class,'overlay')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Sognando Casa")
        item_loader.add_value("landlord_phone", "0552638653")
        item_loader.add_value("landlord_email", "info@sognandocasa.com")


        city = response.xpath("//strong[contains(text(),'Comune')]/following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city)

        floor = response.xpath("//a[contains(text(),'Piano')]/text()").get()
        if floor:
            floor = floor.split(":")[-1].strip()
            item_loader.add_value("floor",floor)

        elevator = response.xpath("//a[text()='Ascensore']/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        yield item_loader.load_item()
