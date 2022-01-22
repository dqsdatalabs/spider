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
    name = 'immotop_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    url = "http://www.immotop.fr/results"

    def start_requests(self):
        start_urls = [
            {
                "formdata" : {
                    'site_frm_select_typop': '2',
                    'site_frm_select_type':'2,appartement'
                },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                    'site_frm_select_typop': '2',
                    'site_frm_select_type':'2,maison'
                },
                "property_type" : "house",
            },
        ]
        for item in start_urls:
            yield FormRequest(self.url,
                            formdata=item["formdata"],
                            dont_filter=True,
                            callback=self.parse,
                            meta={"property_type": item["property_type"], "formdata": item["formdata"]})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 1)
        seen = False
        for item in response.xpath("//ul[contains(@class,'products-list')]//h2[contains(@class,'titre-bien')]//@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Immotop_PySpider_france")

        external_id = response.xpath("//p[contains(@class,'ref')]//text()").get()
        if external_id:
            external_id = external_id.split("ref.")[1].strip()
            item_loader.add_value("external_id", external_id)
        
        title = response.xpath("//h2//text()").get()
        if title:
            item_loader.add_value("title", title.strip(","))
        
        address = response.xpath("//h1//text()").get()
        if address:
            city = address.split("-")[0].strip()
            zipcode = address.split("-")[1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//h2//text()").get()
        if square_meters and "m²" in square_meters:
            square_meters = square_meters.split("m²")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//p[contains(@class,'tarif')]//text()").get()
        if rent:
            rent = rent.strip().replace("€","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//div[contains(@class,'memo')]//text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].split(".")[0].strip()
            if deposit > "0":
                item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//div[contains(@class,'memo')]//text()[contains(.,'Dont')]").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'memo')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//h2//text()").get()
        if room_count and "pièce" in room_count:
            room_count = room_count.split("pièce")[0].strip().split(" ")[-1]
            item_loader.add_value("room_count", room_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'pic-z')]//@src | //div[contains(@class,'small-pics')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'memo')]//text()[contains(.,'Disponible à partir du')]").getall())
        if available_date:
            available_date = available_date.strip().split(" ")[-1].replace(".","")
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        energy_label = response.xpath("//div[contains(@class,'dpe')]//@src[not(contains(.,'dpe_na') or contains(.,'dpe_non'))]").get()
        if energy_label:
            energy_label = energy_label.split("dpe_")[1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", "Immotop")
        item_loader.add_value("landlord_phone", "01 41 58 12 60")
        item_loader.add_value("landlord_email", "contact@immotop.fr")
        
        yield item_loader.load_item()