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
    name = 'elitimo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    form_data = {
        "nature": "2",
        "type": "",
    }
    
    def start_requests(self):
        start_urls = [
            {
                "type": "1",
                "property_type": "apartment"
            },
	        {
                "type": "2",
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            self.form_data["type"] = url.get('type')
            yield FormRequest(url="http://www.elitimo.com/fr/locations",
                            callback=self.parse,
                            formdata=self.form_data,
                            dont_filter=True,
                            meta={
                                'property_type': url.get('property_type'),
                                "type":url.get('type')
                            })

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//ul[contains(@class,'list')]/li/h2//@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Elitimo_PySpider_france")

        external_id = response.xpath("//span[contains(@class,'ref')]//text()").get()
        if external_id:
            external_id = external_id.split("Référence")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1//small//text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//h1//small//text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        square_meters = response.xpath("//li[contains(.,'Surface :')]//text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].split(",")[0].strip()
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//h2[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li[contains(.,'Charges')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//span[contains(@class,'ref')]//parent::p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(.,'Chambre')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(.,'Pièce')]//text()").get()
            if room_count:
                room_count = room_count.split(":")[1].strip().split(" ")[0]
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//li[contains(.,'Salle')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'mainPicture')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//li[contains(.,'Disponible le')]//text()").getall())
        if available_date:
            available_date = available_date.split(":")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@class,'proximities')]//text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'Terrasse')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//div[contains(@class,'services')]//text()[contains(.,'Meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[contains(@class,'services')]//text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//li[contains(.,'Etage')]//text()").get()
        if floor:
            floor = floor.split(":")[1].split("/")[0].strip().split(" ")[0].replace("ème","")
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//img[contains(@alt,'Énergie - Consommation conventionnelle')]//@src").get()
        if energy_label:
            energy_label = energy_label.split("/")[-1][:3]
            if int(energy_label) > 0:
                item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//script[contains(.,'L.marker')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Elitimo")
        item_loader.add_value("landlord_phone", "04 93 87 28 33")

        yield item_loader.load_item()