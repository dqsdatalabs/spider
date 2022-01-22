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
import dateparser
from python_spiders.helper import ItemClear
class MySpider(Spider):
    name = 'cabinetbruno_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.cabinetbruno.com/fr/recherche/"
    current_index = 0
    other_prop = ["2"]
    other_prop_type = ["house"]
    def start_requests(self):
        formdata = {
            "nature": "2",
            "type": "1",
            "rooms": "",
            "city": "",
            "area_min": "",
            "area_max": "",
            "price_min": "",
            "price_max": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for item in response.xpath("//li/div/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if response.xpath("//a[@class='next']/@href").get():
            p_url = f"https://www.cabinetbruno.com/fr/recherche/{page}"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "nature": "2",
                "type": self.other_prop[self.current_index],
                "rooms": "",
                "city": "",
                "area_min": "",
                "area_max": "",
                "price_min": "",
                "price_max": "",
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Cabinetbruno_PySpider_france")
        # item_loader.add_value("external_id", "")

        external_id = response.xpath("//article//p//text()[contains(.,'Référence')]").get()
        if external_id:
            external_id = external_id.split("Référence")[1].strip().split(" ")[0]
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        square_meters = response.xpath("//h2//text()[contains(.,'m²')]").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//h1//i//text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//p[contains(.,'Dépôt de garantie')]//text()").get()
        if deposit:
            deposit = deposit.split("Dépôt de garantie :")[1].split("€")[0].strip()
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//p[contains(.,'Charges :')]//text()").get()
        if utilities:
            utilities = utilities.split("Charges :")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//article//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//h2//text()[contains(.,'pièce')]").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//p[contains(@class,'areas')]//text()[contains(.,'Salle')]").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("Salle")[0].strip().split(" ")[-1]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'picture')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        balcony = response.xpath("//p[contains(@class,'areas')]//text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//p[contains(@class,'areas')]//text()[contains(.,'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//p[contains(@class,'services')]//text()[contains(.,'Meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//p[contains(@class,'services')]//text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        energy_label = response.xpath("//ul[contains(@class,'diagnostics')]//@src").get()
        if energy_label:
            energy_label = energy_label.split("value/")[1].split("/")[0]
            item_loader.add_value("energy_label", energy_label)
            
        swimming_pool = response.xpath("//p[contains(@class,'services')]//text()[contains(.,'Piscine')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        item_loader.add_value("landlord_name", "CABINET BRUNO")
        item_loader.add_value("landlord_phone", "+33 4 93 39 09 81")
        item_loader.add_value("landlord_email", "gestion@cabinetbruno.com")
        yield item_loader.load_item()