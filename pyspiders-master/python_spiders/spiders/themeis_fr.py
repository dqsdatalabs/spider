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
    name = 'themeis_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    url = "https://themeis.fr/fr/recherche"
    formdata = {
        'search-form-22359[search][type_subtype]': '',
        'search-form-22359[search][category]': 'Location|2'
    }
    headers = {
        'authority': 'themeis.fr',
        'cache-control': 'max-age=0',
        'upgrade-insecure-requests': '1',
        'origin': 'https://themeis.fr',
        'content-type': 'application/x-www-form-urlencoded',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'referer': 'https://themeis.fr/fr/recherche',
        'accept-language': 'tr,en;q=0.9'
    }

    def start_requests(self):
        property_types = {"apartment": "Appartement||5", "house": "Maison||18"}
        for k, v in property_types.items():
            self.formdata["search-form-22359[search][type_subtype]"] = v
            yield FormRequest(self.url, headers=self.headers, formdata=self.formdata, dont_filter=True, callback=self.parse, meta={'property_type': k, 'formdata_type': v})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='_list listing']/li//a[contains(.,'Voir le bien')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//li[@class='next']/a/@href").get()
        if next_button:
            headers = {
                'authority': 'themeis.fr',
                'upgrade-insecure-requests': '1',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-user': '?1',
                'sec-fetch-dest': 'document',
                'referer': 'https://themeis.fr/fr/recherche',
                'accept-language': 'tr,en;q=0.9'
            }
            yield Request(response.urljoin(next_button), headers=headers, callback=self.parse, meta={'property_type': response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Themeis_PySpider_france")
        external_id =response.xpath("//li[contains(.,'Référence ')]//span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)

        room_count = response.xpath("//li[contains(.,'Pièces ')]//span//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        
        square_meters = response.xpath("//li[contains(.,'Surface') and not(contains(.,'totale'))]//span//text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0]
            item_loader.add_value("square_meters", int(float(square_meters)) )

        address = response.xpath("//h1//text()").get()
        if address:
            address = address.strip().split(" ")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        rent = response.xpath("//h2[contains(@class,'price')]//text()").get()
        if rent:
            price = rent.split("€")[0].strip().replace("\u202f","").replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        floor = response.xpath("//li[contains(.,'Étage')]//span//text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        
        images = [x for x in response.xpath("//div[contains(@class,'slider')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        utilities = "".join(response.xpath("//li[contains(.,'charges')]//text()").getall())
        if utilities:
            utilities = utilities.split("€")[0].strip().split(" ")[-1]
            item_loader.add_value("utilities", utilities)
        
        deposit = "".join(response.xpath("//li[contains(.,'garantie')]//text()").getall())
        if deposit:
            deposit = deposit.split("€")[0].strip().split(" ")[-1].replace(",","")
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//p[contains(@id,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        

        energy_label = " ".join(response.xpath("//div[contains(@class,'dpe')]//text()").getall())
        if energy_label:
            energy_label = energy_label.split("kWhEP/")[0].strip().split(" ")[-1]
            try:
                item_loader.add_value("energy_label", str(int(float(energy_label))))
            except: pass
            
        item_loader.add_value("landlord_name", "THEMEIS IMMOBILIER")
        item_loader.add_value("landlord_phone", "+33 1 34 83 31 36")
        item_loader.add_value("landlord_email", "themeis@themeis.fr")

        yield item_loader.load_item()