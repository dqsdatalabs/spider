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
    name = 'cadoret_immobilier_pornic_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.cadoret-immobilier.com/location/tous/appartement",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.cadoret-immobilier.com/location/tous/maison",
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
        for item in response.xpath("//article"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )
        
        next_page = response.xpath("//a[contains(@title,'Page suivante')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Cadoret_Immobilier_Pornic_PySpider_france")
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_xpath("external_id", "substring-after(//div/text()[contains(.,'Réf. ')],'Réf. ')")
        item_loader.add_xpath("rent_string", "//h2/text()[contains(.,'€')]")
        deposit = response.xpath("//li[contains(.,'Dépot de garantie :')]/text()[not(contains(.,': 0 €'))]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[-1])
        utilities = response.xpath("//li[contains(.,'Charges :')]/text()[not(contains(.,': 0 €'))]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].strip().split(" ")[0])
        square_meters = response.xpath("//li[contains(.,'Surface habitable :')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[-1].split("m")[0].strip())

        description = "".join(response.xpath("//div[@class='field-items']//p//text()").getall())   
        if description:
            item_loader.add_value("description", description.strip())

        address = response.xpath("//h1/text()").get()
        if address:
            address = address.split(" à ")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[-1].split(")")[0].strip())

        room_count = response.xpath("//li[contains(.,'Nombre de chambre(s) :')]/text() | //li[contains(.,'Nombre de pièce(s) :')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
        bathroom_count = response.xpath("//li[contains(.,'Nombre de salle(s) de bain :')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())
        energy_label = response.xpath("//img[contains(@alt,'DPE')]/@src[not(contains(.,'dpe-ni.png'))]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("/dpe-")[-1].split(".")[0].upper())
        floor = response.xpath("//li[contains(.,'étage(s) : ')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(":")[-1])
        images = [x for x in response.xpath("//div[@id='photos']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Cabinet Cadoret Immobilier Pornic")
        item_loader.add_value("landlord_phone", "02 40 82 55 57")
        yield item_loader.load_item()