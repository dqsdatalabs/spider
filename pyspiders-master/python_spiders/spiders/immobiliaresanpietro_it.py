# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'immobiliaresanpietro_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobiliaresanpietro_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immobiliaresanpietro.it/immobili/affitto/appartamento/?categoria=54327&cerca=1&idC=61733",
                    "https://www.immobiliaresanpietro.it/immobili/affitto/appartamento-monolocale/?categoria=54326&cerca=1&idC=61733",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.immobiliaresanpietro.it/immobili/affitto/attico-mansarda/?categoria=54328&cerca=1&idC=61733",
                    "https://www.immobiliaresanpietro.it/immobili/affitto/rustico/?categoria=54330&cerca=1&idC=61733",
                    "https://www.immobiliaresanpietro.it/immobili/affitto/ville-case-indipendenti-villette/?categoria=54329&cerca=1&idC=61733"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'Vedi scheda')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)


        external_id = response.xpath(
            "//div[@class='modal-body']//span[contains(.,'Rif.')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Rif."))

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)
            address = title.split(" a ")[-1].split("-")[0]
            if address:
                item_loader.add_value("address",address)

        description = response.xpath(
            "//article[@class='bg-gray-light mb-4 p-4 mx-lg-3 announcement__text']//p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//h2[contains(@class,'announcement__header__price h3')]//text()[contains(.,'€')]").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1])
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath(
            "//h2[contains(@class,'announcement__header__price h3')]//small//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("mq.")[0])

        bathroom_count = response.xpath(
            "//ul[@class='specs list-inline list-unstyled mt-4 mb-4 mt-lg-0 bg-gray-light mx-lg-3']//li[@class='col-6 col-md-3 col-xl-2']//text()[contains(.,'bagno')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bagno"))
        else:
            bathroom_count = response.xpath("//h2[@class='announcement__header__price h3']/small/text()").get()
            if bathroom_count:
                bathroom_count = bathroom_count.split("/")[-1].split()[0].strip()
                item_loader.add_value("bathroom_count",bathroom_count)

        energy_label = response.xpath(
            "//ul[@class='specs list-inline list-unstyled mt-4 mb-4 mt-lg-0 bg-gray-light mx-lg-3']//li[@class='col-6 col-md-3 col-xl-2']//text()[contains(.,'Classe')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].split("-")[0])

        images = response.xpath("//img[@class='w-100 lazyload']/@data-src").getall()
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Immobiliare San Pietro")
        item_loader.add_value("landlord_phone", " 051.04.20.586")

        room_count = response.xpath("//li/span[contains(text(),'camere') or contains(text(),'camera')]/text()").get()
        if room_count:
            room_count = room_count.split()[0]
            item_loader.add_value("room_count",room_count)

        position = response.xpath("//script[contains(text(),'lat:')]/text()").get()
        if position:
            lat = position.split("lat:")[-1].split(",")[0].strip()
            item_loader.add_value("latitude",lat)
            long = position.split("lng:")[-1].split("}")[0].strip()
            item_loader.add_value("longitude",long)

        item_loader.add_value("city","San Pietro")

        yield item_loader.load_item()