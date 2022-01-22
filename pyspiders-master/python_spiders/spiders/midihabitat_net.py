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
    name = 'midihabitat_net'
    execution_type='testing'
    country='france'
    locale='fr'
    current_index = 0
    other_prop = ["2"]
    other_prop_type = ["house"]
    
    custom_settings={"PROXY_ON": True}
    
    def start_requests(self):
        formdata = {
            "location_search[commune]": "",
            "location_search[typeBien][]": "1",
            "location_search[loyer_min]": "0",
            "location_search[loyer_max]": "1000000",
            "location_search[rayonCommune]": "0",
            "location_search[surface_min]": "",
            "location_search[noMandat]": "",
            "location_search[tri]": "loyerCcTtcMensuel|asc",
        }
        
        headers = {
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        }
        
        yield FormRequest(
            url="https://www.midihabitat.net/fr/locations",
            callback=self.parse,
            formdata=formdata,
            headers=headers,
            meta={
                "property_type":"apartment",
                "formdata":formdata,
            }
        )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='informations_bien']/h2/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        page = response.xpath("//div[@id='plus-bien']/@data-nb-biens").get()
        if page:
            yield Request(
                url=f"https://www.midihabitat.net/fr/map/mini-fiche/Location/{page}/normal/loyerCcTtcMensuel%7Casc",
                callback=self.parse,
                meta={"property_type":response.meta["property_type"],"formdata":response.meta["formdata"]})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "location_search[commune]": "",
                "location_search[typeBien][]": self.other_prop[self.current_index],
                "location_search[loyer_min]": "0",
                "location_search[loyer_max]": "1000000",
                "location_search[rayonCommune]": "0",
                "location_search[surface_min]": "",
                "location_search[noMandat]": "",
                "location_search[tri]": "loyerCcTtcMensuel|asc",
            }
            yield FormRequest(
                url="https://www.midihabitat.net/fr/locations",
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                    "formdata":formdata,
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Midihabitat_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="normalize-space(//div[@class='titre']/h3/text())", input_type="F_XPATH", split_list={"|":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="normalize-space(//div[@class='titre']/h3/text())", input_type="F_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="normalize-space(//div[@class='titre']/h3/text())", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="normalize-space(//div[@class='titre']/h3/text()[contains(.,'Réf')])", input_type="F_XPATH", split_list={"Réf. :":1, "|":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='criteres']/div[contains(.,'habitable')]/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        
        if response.xpath("//p[@class='chambre']/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[@class='chambre']/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//h2[@class='clr-location']/text()", input_type="F_XPATH", get_num=True, split_list={"pièce":0, " ":-1})
            
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[@class='sdb']/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'prix')]/text()", input_type="M_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='criteres']/div[contains(.,'garantie')]/text()", input_type="F_XPATH", get_num=True, split_list={":":1}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='carousel-photo']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'position = [')]/text()", input_type="F_XPATH", split_list={"position = [":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'position = [')]/text()", input_type="F_XPATH", split_list={"position = [":1, ",":1, "]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[@class='criteres']/div[contains(.,'Étage')]/text()", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p[@class='parking']/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@class='criteres']/div[contains(.,'Terrasse')]/text()[not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        
        desc = " ".join(response.xpath("//div[@class='descriptif']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//div[@class='valeur_conso'][contains(.,'*')]/text()").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[0]
            item_loader.add_value("energy_label", energy_label)
        
        available_date = ""
        if "disponible le" in desc:
            available_date = desc.split("disponible le")[1].replace("N'",".").split(".")[0].strip()
        elif "disponible à partir du" in desc:
            available_date = desc.split("disponible à partir du")[1].replace("N'",".").split(".")[0].strip()
        
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="MIDIHABITAT Services Immobiliers", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05 62 73 68 73", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@midihabitat.net", input_type="VALUE")

        yield item_loader.load_item()