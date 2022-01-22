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
    name = 'apally_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.apally.com/catalog/advanced_search_result.php?action=update_search&search_id=1694031159158502&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&&search_id=1694031159158502&page=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.apally.com/catalog/advanced_search_result.php?action=update_search&search_id=1694031159158502&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2%2C17&C_27_tmp=17&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&&search_id=1694031159158502&page=1",
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

        for item in response.xpath("//div[@id='listing_bien']/div"):
            follow_url = response.urljoin(item.xpath("./div[contains(@class,'padding')]/a/@href").get())
            let_agreed = item.xpath(".//img[contains(@src,'loue')]").get()
            if not let_agreed: yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[@class='page_suivante']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Apally_PySpider_france")

        external_id = response.xpath("//span[contains(.,'Référence')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//title//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(.,'Ville')]//text()").get()
        if address:
            address = address.split(":")[1].strip()
            item_loader.add_value("address", address.strip())

        city = response.xpath("//span[contains(.,'Ville')]//text()").get()
        if city:
            city = city.split(":")[1].strip()
            item_loader.add_value("city", city.strip())

        zipcode = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Code')]//following-sibling::div//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Surface')]//following-sibling::div//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(".")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Loyer')]//following-sibling::div//text()").get()
        if rent:
            rent = rent.strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Dépôt de Garantie')]//following-sibling::div//text()").get()
        if deposit:
            deposit = deposit.strip().split(" ")[0]
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Provision sur charges')]//following-sibling::div//text()").get()
        if utilities:
            utilities = utilities.strip().replace("€","").replace(" ","")
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//div[contains(@class,'content_details_description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(.,'Chambre')]//text()").get()
        if room_count:
            room_count = room_count.split(":")[1].strip()
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                room_count = response.xpath("//span[contains(.,'Pièce')]//text()").get()
                if room_count:
                    room_count = room_count.split(":")[1].strip()
                    item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//span[contains(.,'Pièce')]//text()").get()
            if room_count:
                room_count = room_count.split(":")[1].strip()
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Salle')]//following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'flexslider')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Disponibilité')]//following-sibling::div//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'garage') or contains(.,'parking')]//following-sibling::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Meublé')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Ascenseur')]//following-sibling::div//text()[contains(.,'Oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Etage')]//following-sibling::div//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//li[contains(@class,'list-group-item')]//div[contains(.,'Conso Energ')]//following-sibling::div//text()[not(contains(.,'Vierge'))]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "APALLY")
        item_loader.add_value("landlord_phone", "02 37 45 48 48")
        item_loader.add_value("landlord_email", "cecilia.barbier@apally.com")

        yield item_loader.load_item()