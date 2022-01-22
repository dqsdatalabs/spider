# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'cabinet_courel_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cabinet-courel.fr/recherche?a=2&b%5B%5D=appt&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cabinet-courel.fr/recherche?a=2&b%5B%5D=house&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 12)
        seen = False
        for item in response.xpath("//a[@class='res_tbl1']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            if response.meta["property_type"] == "apartment":
                p_url = f"https://www.cabinet-courel.fr/search.php?a=2&b%5B%5D=appt&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&transact=&neuf=&agence=&view=&ajax=1&facebook=1&start={page}&&_=1613564556799"
                yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+12})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source","Cabinet_Courel_PySpider_france")
        external_id = response.xpath("//td[contains(@class,'l1')][contains(.,'Référence')]//following-sibling::td//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title",title)

        address = "".join(response.xpath("//td[contains(.,'Ville')]/following-sibling::td//text()").getall())
        if address:
            zipcode = address.split(" ")[-1]
            city = address.split(zipcode)[0]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        desc = " ".join(response.xpath("//div[contains(@itemprop,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        rent = "".join(response.xpath("//td[contains(@itemprop,'price')]//text()").getall())
        if rent:
            rent = rent.split("€")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = "".join(response.xpath("//div[contains(@class,'basic_copro')]//text()[contains(.,'Dépôt de garantie')]").getall())
        if deposit :
            deposit = deposit.split("Dépôt de garantie")[1].split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        utilities = "".join(response.xpath("//div[contains(@class,'basic_copro')]//text()[contains(.,'Provision sur charges')]").getall())
        if utilities :
            utilities = utilities.split("Provision sur charges")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        else:
            utilities = "".join(response.xpath("//div[contains(@id,'details')]//text()[contains(.,'Charges ')]").getall())
            if utilities :
                utilities = utilities.split("Charges :")[1].split("/")[0].strip()
                item_loader.add_value("utilities", utilities)
        
        square_meters = "".join(response.xpath("//td[contains(.,'Surface')]/following-sibling::td//text()").getall())
        if square_meters:
            square_meters = square_meters.split(" ")[0].strip()
            if square_meters:
                item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//td[contains(.,'Chambres')]/following-sibling::td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//td[contains(.,'Pièces')]/following-sibling::td//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
            
        bathroom_count = response.xpath("//td[contains(.,'Salle')]/following-sibling::td//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//td[contains(.,'Disponibilité')]/following-sibling::td//text()").getall())
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        floor = response.xpath("//td[contains(.,'Étage')]/following-sibling::td//text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        balcony = response.xpath("//td[contains(.,'Balcon')]/following-sibling::td//text()[contains(.,'Oui')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//td[contains(.,'Stationnement')]/following-sibling::td//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//td[contains(.,'Ascenseur')]/following-sibling::td//text()[contains(.,'Oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        energy_label = response.xpath("//b[contains(@class,'dpe-letter-active')]//text()").get()
        if energy_label:
            energy_label = energy_label.split(":")[0].strip()
            item_loader.add_value("energy_label", energy_label)

        images = [x for x in response.xpath("//a[contains(@class,'rsImg')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Cabinet COUREL")
        item_loader.add_value("landlord_phone", "02 35 73 58 50")
        item_loader.add_value("landlord_email", "contact@cabinet-courel.fr")
        
        yield item_loader.load_item()