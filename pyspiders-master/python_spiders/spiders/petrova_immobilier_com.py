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
    name = 'petrova_immobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    post_url = "http://www.petrova-immobilier.com/fr/search/"
    current_index = 0
    other_prop = ["2"]
    other_type = ["house"]
    def start_requests(self):
        formdata = {
            "nature": "2",
            "type": "1",
            "rooms": "",
            "city": "",
            "price": "",
            "homepage": "1",
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for url in response.xpath("//h2/a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if response.xpath("//a[@class='next']/@href").get():
            p_url = f"http://www.petrova-immobilier.com/fr/search/{page}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "nature": "2",
                "type": self.other_prop[self.current_index],
                "rooms": "",
                "city": "",
                "price": "",
                "homepage": "1",
            }
            yield FormRequest(self.post_url,
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'property_type': self.other_type[self.current_index],})
            self.current_index += 1

                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Petrova_Immobilier_PySpider_france")
        
        external_id = response.xpath("//span[contains(@class,'reference')]//text()").get()
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
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath("//h2[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.strip().split("€")[0].replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]//text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].replace(" ","").strip()
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li[contains(.,'Charges :')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        else:
            utilities = response.xpath("//p/text()[contains(.,'charges')]").get()
            if utilities:
                utilities = utilities.lower().split("charges")[1].replace(":","").replace("€","").replace("e,","").replace(",",".").strip()
                if utilities.split(" ")[0].replace(".","").isdigit():
                    item_loader.add_value("utilities", int(float(utilities.split(" ")[0])))
        
        desc = " ".join(response.xpath("//div[contains(@class,'content')]/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(.,'Chambres')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        elif response.xpath("//li[contains(.,'Chambre')]//text()").get():
            room_count = response.xpath("//li[contains(.,'Chambre')]//text()").get()
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)            
        else:
            room_count = response.xpath("//li[contains(.,'Pièces')]//text()").get()
            if room_count:
                room_count = room_count.split(":")[1].strip().split(" ")[0]
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(.,'Salle de')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'mainPicture')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'Garage')]//text()").get()
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

        swimming_pool = response.xpath("//li[contains(.,'Pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        floor = response.xpath("//li[contains(.,'Etage')]//text()").get()
        if floor:
            floor = floor.split(":")[1].split("/")[0].strip().replace("\u00e8me","").replace("\u00e9tage","")
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//img[contains(@alt,'Énergie - Consommation conventionnelle')]//@src").get()
        if energy_label:
            energy_label = energy_label.split("/")[-1].split("%")[0]
            if energy_label > "0":
                item_loader.add_value("energy_label", energy_label)

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//li[contains(.,'Disponible le')]//text()").getall())
        if available_date:
            available_date = available_date.split(":")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        latitude_longitude = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0]
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Petrova Immobilier")
        item_loader.add_value("landlord_phone", "04 93 92 05 58")
        item_loader.add_value("landlord_email", "agence@petrovaimmobilier.com ")

        yield item_loader.load_item()