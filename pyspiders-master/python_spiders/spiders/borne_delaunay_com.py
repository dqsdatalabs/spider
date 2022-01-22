# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re 

class MySpider(Spider):
    name = 'borne_delaunay_com'
    execution_type='testing'
    country='france'
    locale='fr' 

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.borne-delaunay.com/nos-biens-immobiliers?utf8=%E2%9C%93&t=rent&acc_t%5B%5D=appartment&commit=Actualiser&a_min=&a_max=&p_min=&p_max=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.borne-delaunay.com/nos-biens-immobiliers?utf8=%E2%9C%93&t=rent&acc_t%5B%5D=house&commit=Actualiser&a_min=&a_max=&p_min=&p_max=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        for item in response.xpath("//a[@class='thumb__image img-ratio-wrapper relative']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[@rel='next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Borne_Delaunay_PySpider_france")
        item_loader.add_value("external_id", response.url.split("-")[-1])

        title = " ".join(response.xpath("//h1[contains(@class,'card--accs__title')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = " ".join(response.xpath("//td[contains(.,'Adresse')]//following-sibling::td//text() | //div[contains(@class,'card--accs__zipcode')]//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        city_zipcode = response.xpath("//div[contains(@class,'card--accs__zipcode')]//text()").get()
        if city_zipcode:
            zipcode = city_zipcode.strip().split(" ")[-1]
            city = city_zipcode.split(zipcode)[0]
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//td[contains(.,'Surface')]//following-sibling::td//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath("//h3[contains(@class,'price')]/text()").get()
        if rent:
            rent = rent.strip().replace("€","").replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//td[contains(.,'Dépôt de garantie')]//following-sibling::td//text()").get()
        if deposit:
            deposit = deposit.strip().replace("€","").replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//td[contains(.,'Charge')]//following-sibling::td//text()").get()
        if utilities:
            utilities = utilities.strip().replace("€","")
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'accommodation-show__text')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//td[contains(.,'Chambre')]//following-sibling::td//text()").get()
        if room_count and room_count > "0":
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//td[contains(.,'Pièce')]//following-sibling::td//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//td[contains(.,'Salle')]//following-sibling::td//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'accommodation-show__image')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//td[contains(.,'Parking')]//following-sibling::td//text()[contains(.,'oui')]").get()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//td[contains(.,'Ascenseur')]//following-sibling::td//text()[contains(.,'oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//td[contains(.,'Etage')]//following-sibling::td//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//div[contains(@class,'list-energy')]//span//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        latlng="".join(response.xpath("//div[@class='map-wrapper']/custom-map").extract())
        if latlng:
            latitude=latlng.split("{lat:")[-1].split(",")[0].strip()
            longitude=latlng.split(", lng:")[-1].split("}")[0].strip()
            item_loader.add_value("latitude",latitude)
            item_loader.add_value("longitude",longitude)

        item_loader.add_value("landlord_name", "Borne & Delaunay")
        item_loader.add_value("landlord_phone", "04 93 62 26 66")
        item_loader.add_value("landlord_email", "agence@borne-delaunay.com")

        yield item_loader.load_item()