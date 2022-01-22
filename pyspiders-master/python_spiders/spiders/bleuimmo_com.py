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
    name = 'bleuimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    post_url = "https://www.bleuimmo.com/annonces-immobilieres/recherche-multicritere/listing.html"
    current_index = 0
    other_prop = ["1"]
    other_prop_type = ["studio"]
    def start_requests(self):
        formdata = {
            "origin": "50",
            "page": "1",
            "sort": "2",
            "sort_invert": "2",
            "typeannonce": "2",
            "typebiens[]": "99",
            "prixmax": "",
            "surf_hab_max": "",
            "nb_piec_min": "",
            "del_ann": "0",
            "etendre_selection": "0",
            "prixmin": "",
            "surf_terr_min": "",
            "nb_ch_min": "",
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
        seen = False
        for item in response.xpath("//figure//button[@type='button']/@onclick").getall():
            url = "".join(item.split(".href = ")[1].split(";")[0].strip().replace("'", "").split("+")).replace(" ", "")
            follow_url = response.urljoin(url)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 2 or seen:
            p_url = f"https://www.bleuimmo.com/annonces-immobilieres/recherche-multicritere/listing-{page}.html"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "origin": "50",
                "page": "1",
                "sort": "2",
                "sort_invert": "2",
                "typeannonce": "2",
                "typebiens[]": self.other_prop_type[self.current_index],
                "prixmax": "",
                "surf_hab_max": "",
                "nb_piec_min": "",
                "del_ann": "0",
                "etendre_selection": "0",
                "prixmin": "",
                "surf_terr_min": "",
                "nb_ch_min": "",
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

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Bleuimmo_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h3/text()[1]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h3/text()[1]", input_type="F_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h3/text()[1]", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="normalize-space(//h1/text())", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@class,'desc-text')]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="substring-before(substring-after(//li[contains(.,'habitable')]/text(),':'),'m')", input_type="F_XPATH", get_num=True)
        
        if response.xpath("//h1/text()[contains(.,'Studio')]"):
            item_loader.add_value("room_count", "1")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="substring-after(//li[contains(.,'chambre')]/text(),':')", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h3/b/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="substring-after(//li[contains(.,'Dépôt ')]/text(),':')", input_type="F_XPATH", get_num=True)
        
        external_id = response.xpath("//p[contains(@class,'reference')]/text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip().replace(" / ", "/").split(" ")[0]
            item_loader.add_value("external_id", external_id)
        
        available_date = response.xpath("//p[contains(@class,'desc-text')]/text()[contains(.,'Disponible')]").get()
        if available_date:
            available_date = available_date.split(" du ")[1].split("à")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slides']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="substring-after(//li[contains(.,'Montant charge')]/text(),':')", input_type="F_XPATH", get_num=True)
        
        energy_label = response.xpath("//div[@class='dpe-blck']//li[contains(@class,'selected')]//span[contains(.,'D =')]/text()").get()
        if energy_label:
            energy_label = energy_label.split("=")[1].strip()
            item_loader.add_value("energy_label", energy_label)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="BLEU IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 68 82 78 38", input_type="VALUE")

        yield item_loader.load_item()