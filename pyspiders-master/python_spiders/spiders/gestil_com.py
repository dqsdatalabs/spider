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
    name = 'gestil_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.gestil.com/resultats?transac=location&type%5B%5D=appartement&budget_maxi=&surface_mini=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.gestil.com/resultats?transac=location&type%5B%5D=maison&budget_maxi=&surface_mini=",
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
        
        script = response.xpath("//div[contains(@class,'properties-listing')]/script/text()").get()
        if script:
            for item in script.split('"lien": "'):
                query = item.split('"')[0]
                if "location" in query:
                    follow_url = response.urljoin(query)
                    yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Gestil_PySpider_france")
        item_loader.add_value("external_id", response.url.split("-")[-1])
        
        title = "".join(response.xpath("//h1[contains(@class,'titre')]//text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        
        square_meters = response.xpath("//li[contains(.,'Habitable')]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//li[contains(.,'chambre')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(.,'pièce')]/strong/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[contains(.,'Salle')]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        address = response.xpath("//li/address/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
        
        rent = response.xpath("//h3/span/text()[contains(.,'€')]").get()
        if rent:
            price = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        floor = response.xpath("//li[contains(.,'Etage')]/strong/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        utilities = response.xpath("//li[contains(.,'Charges')]/strong/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//li[contains(.,'Dépôt')]/strong/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'Garage')]/strong/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//li[contains(.,'terrasse')]/strong/text()[.!='0']").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]/strong/text()[.!='0']").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//li[contains(.,'Meublé')]/strong/text()[.!='0']").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//li[contains(.,'Ascenseur')]/strong/text()[contains(.,'Oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        energy_label = response.xpath("//img[@alt='DPE']/@src").get()
        if energy_label:
            energy_label = energy_label.split("DPE_")[1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)
        
        description = " ".join(response.xpath("//div[@class='col-sm-8']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        import dateparser
        if "Disponible début" in description:
            available_date = description.split("Disponible début")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slides']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Gestil IMMOBILIER")
        item_loader.add_value("landlord_phone", "01 30 38 95 85")
        item_loader.add_value("landlord_email", "info@gestil.com")
        
        yield item_loader.load_item()