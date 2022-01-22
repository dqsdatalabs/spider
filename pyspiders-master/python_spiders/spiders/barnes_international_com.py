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
class MySpider(Spider):
    name = 'barnes_international_com'
    execution_type='testing'
    country='france'
    locale='fr'
    post_url = "https://www.barnes-international.com/fr/location/france/"
    current_index = 0
    other_prop = ["maison/villa"]
    other_prop_type = ["house"]
    def start_requests(self):
        formdata = {
            "FormSearchPays": "FR",
            "FormSearchTypeannonce": "location",
            "typebien[]": "appartement",
            "prix_min": "",
            "prix_max": "",
            "id_ref_go": "",
            "surf_min": "",
            "surf_max": "",
            "FormSearchPaysOld": "FR",
            "post_redirect_get": "y",
            "FormSearchLocalisation": "",
            "LocalisationchoisiIdBloc": "0",
            "req_tri": "DEFAULT",
            "req_typerecherche": "liste",
            "select_lang": "FR",
            "FormSearchLocalisation_intern": "",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            formdata=formdata,
            meta={
                "property_type":"apartment",
            }
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 28)
        seen = False
        for item in response.xpath("//a[contains(.,'Découvrir ce bien')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 28 or seen:
            p_url = f"https://www.barnes-international.com/v5_php/moteur.php?ajx=ok&start={page}"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+28})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "FormSearchPays": "FR",
                "FormSearchTypeannonce": "location",
                "typebien[]": self.other_prop[self.current_index],
                "prix_min": "",
                "prix_max": "",
                "id_ref_go": "",
                "surf_min": "",
                "surf_max": "",
                "FormSearchPaysOld": "FR",
                "post_redirect_get": "y",
                "FormSearchLocalisation": "",
                "LocalisationchoisiIdBloc": "0",
                "req_tri": "DEFAULT",
                "req_typerecherche": "liste",
                "select_lang": "FR",
                "FormSearchLocalisation_intern": "",
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
        item_loader.add_value("external_source", "Barnes_International_PySpider_france")
        item_loader.add_xpath("title", "//h1//text()")
        item_loader.add_xpath("external_id", "substring-after(//div//text()[contains(.,'Réf ')],'Réf ')")
        deposit = response.xpath("//div//text()[contains(.,'Dépôt de garantie :') and contains(.,'€')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[-1].replace(" ",""))
        utilities = response.xpath("//div//text()[contains(.,'charges locatives') and contains(.,'€')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].replace(" ",""))
        rent = response.xpath("//li[div[.='Loyer']]/div[2]/text()[contains(.,'€')]").get()
        if rent:
            item_loader.add_value("rent", rent.replace(" ",""))
        item_loader.add_value("currency", "EUR")
        
        item_loader.add_xpath("square_meters", "//li[div[.='Surface']]/div[2]/text()")

        description = "".join(response.xpath("//div[@class='col-lg-7']/p//text()").getall())   
        if description:
            item_loader.add_value("description", description)

        address = response.xpath("//li[div[.='Ville']]/div[2]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city"," ".join(address.split(" ")[:-1]).strip())
            item_loader.add_value("zipcode", address.strip().split(" ")[-1])

        room_count = response.xpath("//li[div[.='Chambres' or .='Chambre']]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[div[contains(.,'Pièce')]]/div[2]/text()")
        bathroom_count = response.xpath("//li[div[.='Salles de bains' or .='Salle de bains']]/div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        else:
            item_loader.add_xpath("bathroom_count", "//li[div[.='Salle de douche']]/div[2]/text()")
        energy_label = response.xpath("//li[div[.='DPE']]/div[2]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("(")[0].strip())
        floor = response.xpath("//li[div[.='Étage']]/div[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("ème")[0])
        images = [x for x in response.xpath("//div[@class='item d-block ']//picture//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        furnished = response.xpath("//li[div[.='Meublé']]/div[2]/text()").get()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        elevator = response.xpath("//li[div[.='Ascenseur']]/div[2]/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        elevator = response.xpath("//li[div[.='Ascenseur']]/div[2]/text()").get()
        if elevator:
            if "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        washing_machine = response.xpath("//li[div[.='Lave-linge']]/div[2]/text()").get()
        if washing_machine:
            if "oui" in washing_machine.lower():
                item_loader.add_value("washing_machine", True)
            else:
                item_loader.add_value("washing_machine", False)
        dishwasher = response.xpath("//li[div[.='Lave-vaisselle']]/div[2]/text()").get()
        if dishwasher:
            if "oui" in dishwasher.lower():
                item_loader.add_value("dishwasher", True)
            else:
                item_loader.add_value("dishwasher", False)
        terrace = response.xpath("//div/p/text()[contains(.,'TERRASSE')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        script_map = response.xpath("//script[contains(.,'google.maps.LatLng(')]/text()").get()
        if script_map:
            latlng = script_map.split("google.maps.LatLng(")[1].split(");")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        landlord_name = response.xpath("//div[@class='contact__infos pt-0']//p[@class='h3']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "BARNES INTERNATIONAL REALTY")

        item_loader.add_xpath("landlord_phone", "//div[@class='contact__infos pt-0']//p[@class='h3']/span/text()")
        yield item_loader.load_item()