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
    name = 'lmimmobiliercaen_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lmimmobiliercaen.fr/property-search/?status=location&type=appartement",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.lmimmobiliercaen.fr/property-search/?status=location&type=maison",
                    "https://www.lmimmobiliercaen.fr/property-search/?status=location&type=ensemble-immobilier",
                    "https://www.lmimmobiliercaen.fr/property-search/?status=location&type=immeuble"
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//figure/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page==2 or seen:
            f_url = f"https://www.lmimmobiliercaen.fr/property-search/page/{page}/?status=location&type={response.url.split('=')[-1]}"
            yield Request(f_url, callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Lmimmobiliercaen_PySpider_france")

        external_id = response.xpath("//span[contains(@title,'ID')]/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)     
        
        title = " ".join(response.xpath("//title//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//address//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//address//text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        square_meters = response.xpath("//span[contains(@title,'Surface')]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(",")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))

        rent = "".join(response.xpath("//span[contains(@class,'price')]/text()").getall())
        if rent:
            rent = rent.strip().split("€")[0].replace(" ","").strip().replace("\xa0","")
            print()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//p[contains(.,'Dépôt de garantie')]//text()").get()
        if deposit:
            deposit = deposit.split("Dépôt de garantie")[1].split("€")[0].split("de ")[-1].replace(":","").replace(".","").strip().replace(" ","")
            item_loader.add_value("deposit", int(float(deposit)))

        utilities = response.xpath("//p[contains(.,'dont')]//text()").get()
        if utilities:
            if "+" in utilities:
                utilities = utilities.split("+")[1].split("€")[0].strip()
                item_loader.add_value("utilities", utilities)
            else:
                utilities = utilities.split("dont")[1].split("€")[0].split(",")[0].strip()
                item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'content clearfix')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(@class,'bed')]/text()").get()
        if room_count:
            room_count = room_count.strip().split("\u00a0")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@class,'bath')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split("\u00a0")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//ul[contains(@class,'slides')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//li[contains(@id,'rh_property__feature')][contains(.,'Meublé') or contains(.,'meublé')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        parking = response.xpath("//span[contains(@class,'garage')]/text()").get()
        if parking:
            if "0" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        energy_label = "".join(response.xpath("//span[contains(@class,'diagnostic-number')]/text()").getall())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('lng":"')[1].split('"')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "LM IMMOBILIER")
        item_loader.add_value("landlord_phone", "02 31 712 712")
        item_loader.add_value("landlord_email", "contact@lmimmobiliercaen.fr")
     
        yield item_loader.load_item()