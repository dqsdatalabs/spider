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
    name = 'auriol_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://locations.auriol-immobilier.com/appartements-croissant-1.html", "property_type": "apartment"},
	        {"url": "https://locations.auriol-immobilier.com/maisons-et-villas-croissant-1.html", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='ann-titre']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Auriol_Immobilier_PySpider_france")
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
            
            if " - " in title:
                address = title.split(" - ")[0].strip()
                item_loader.add_value("address", address)
                zipcode = ""
                for i in address.split(" "):
                    if i.isdigit():
                        zipcode = i
                        break
                if zipcode:
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("city", address.split(zipcode)[0].strip())
                else: item_loader.add_value("city", address)
            else: 
                item_loader.add_value("address", title.split(" ")[0])
                item_loader.add_value("city", title.split(" ")[0])
        
        rent = response.xpath("//span[@class='ann-prix']/span/text()").get()
        if rent:
            rent = rent.split(",")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("substring-after(//span[@class='ann-ref']/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        square_meters = response.xpath("//td[contains(.,'habitable')]/following-sibling::td/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//td[contains(.,'chambre')]/following-sibling::td/text()").get()
        if room_count:
            if room_count.split(" ")[0].isdigit():
                item_loader.add_value("room_count", room_count.split(" ")[0])
            else: room_count = "" 
        if not room_count:
            room_count = response.xpath("//td[contains(.,'pièces')]/following-sibling::td/text()").get()
            if room_count:
                room_count = room_count.split("pièce")[0].strip()
                if room_count.isdigit():
                    item_loader.add_value("room_count", room_count)
                elif "/" in room_count:
                    item_loader.add_value("room_count", room_count.split("/")[0])
                    
        bathroom_count = response.xpath("//td[contains(.,'Salle')]/following-sibling::td/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0]
            if "une" in bathroom_count.lower():
                item_loader.add_value("bathroom_count", "1")
            elif bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                item_loader.add_value("bathroom_count", "1")
        
        utilities = response.xpath("//td[contains(.,'Charge')]/following-sibling::td/text()").get()
        if utilities:
            if "€" in utilities:
                utilities = utilities.split("€")[0].strip()
                item_loader.add_value("utilities", int(float(utilities)))
            elif "euros" in utilities.lower():
                utilities = utilities.split("euros")[0]
                item_loader.add_value("utilities", int(float(utilities)))
        
        floor = response.xpath("//td[contains(.,'Étage')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(" ")[0])
        
        energy_label = response.xpath("//div[@id='dpe']/@data").get()
        if energy_label:
            energy_label = energy_label.split(":")[1].split(";")[0]
            item_loader.add_value("energy_label", energy_label)
        
        available_date = response.xpath("//div[contains(@class,'txt_contenu')]//text()[contains(.,'Disponible') or contains(.,'Libre')]").get()
        if available_date:
            if "Libre le" in available_date:
                available_date = available_date.split("Libre le")[1].strip().replace(".","")
            elif "Disponible" in available_date:
                available_date = available_date.split("Disponible")[1].replace("dés le","").replace("début","").strip().replace(".","")
            import dateparser
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)  
        
        parking = response.xpath("//td[contains(.,'Garage')]/following-sibling::td/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//td[contains(.,'Terrasse')]/following-sibling::td/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        description = " ".join(response.xpath("//div[contains(@class,'txt_contenu')]//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[@class='miniatures']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "AURIOL IMMOBILIER")
        item_loader.add_value("landlord_phone", "0442047059")
        item_loader.add_value("landlord_email", "location@auriol-immobilier.com")
        
        yield item_loader.load_item()