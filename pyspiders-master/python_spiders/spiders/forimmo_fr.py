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
    name = 'forimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Forimmo_PySpider_france'
    def start_requests(self):
        start_urls = [
            {"url": "http://forimmo.fr/resultat.php?transac=location&type=appartement&ville=&budget_mini=0&budget_maxi=1000000&surface_mini=0&surface_maxi=500", "property_type": "apartment"},
	        {"url": "http://forimmo.fr/resultat.php?transac=location&type=maison&ville=&budget_mini=0&budget_maxi=1000000&surface_mini=0&surface_maxi=500", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(
                url=url.get('url'),
                callback=self.parse,
                meta={
                    'property_type': url.get('property_type')
                }
            )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(@class,'txtBtn')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={
                    'property_type': response.meta.get('property_type')
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Forimmo_PySpider_france")
        
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        rent = response.xpath("//span[contains(.,'LOYER')]/text()").get()
        if rent:
            price = rent.split(":")[1].split("€")[0].strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("substring-after(//li[contains(.,'Référence ')]/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        room_count = response.xpath("//div[contains(@class,'txtContentBig')]//text()[contains(.,'chambre')]/following-sibling::b/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("substring-after(//li[contains(.,'pièce')]/text(),':')").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
        
        square_meters = response.xpath("substring-after(//li[contains(.,'habitable')]/text(),':')").get()
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0]
            item_loader.add_value("square_meters", square_meters.strip())
        
        address = response.xpath("substring-after(//li[contains(.,'Localisation')]/text(),':')").get()
        if address:
            address = address.strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0].strip())
        
        utilities = response.xpath("//li[contains(.,'Charges')]/strong/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//li[contains(.,'garantie')]/strong/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].replace(" ","").strip()
            item_loader.add_value("deposit", deposit)
        
        floor = response.xpath("//div[contains(@class,'txtContentBig')]//text()[contains(.,'Etage')]/following-sibling::b/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        bathroom_count = response.xpath("//div[contains(@class,'txtContentBig')]//text()[contains(.,'Salle')]/following-sibling::b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        description = " ".join(response.xpath("//div[contains(@class,'txtContentBig')]//text()[1]").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//ul[@id='thumbs']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = response.xpath("//img[contains(@src,'DPE_')]/@src").get()
        if energy_label:
            energy_label = energy_label.split("DPE_")[1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)
        
        item_loader.add_value("landlord_name", "FORIMMO AGENCE IMMOBILIERE")
        item_loader.add_value("landlord_phone", "04-92-00-80-47")
        item_loader.add_value("landlord_email", " location.forimmo@gmail.com")
        
        yield item_loader.load_item()