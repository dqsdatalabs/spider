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
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'gti_gpi_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.gti-gpi.com/fr/locations-biens-immobiliers.htm?_typebase=2&_typebien%5B%5D=1&_motsclefs=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.gti-gpi.com/fr/locations-biens-immobiliers.htm?_typebase=2&_typebien%5B%5D=2&_motsclefs=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='item-image']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Gti_Gpi_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//font[contains(.,'€ /mois')]//parent::div/text()", input_type="M_XPATH", split_list={"Réf.":1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[contains(@id,'lieu-detail')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[contains(@id,'lieu-detail')]//text()", input_type="F_XPATH", split_list={"-":1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@id,'lieu-detail')]//text()", input_type="F_XPATH", split_list={"-":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@id,'texte-detail')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[contains(@class,'champsSPEC-element')][contains(.,'Surface habitable')]//following-sibling::div//text()", input_type="F_XPATH", get_num=True, split_list={"m":0,",":0})
        if response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'Chambre')]//following-sibling::div//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'champsSPEC-element')][contains(.,'Chambre')]//following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        if response.xpath("//div[contains(@class,'champsSPEC-element')][contains(.,'Pièce')]//following-sibling::div//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'champsSPEC-element')][contains(.,'Pièce')]//following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[contains(@class,'champsSPEC-element')][contains(.,'Salle')]//following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//font[contains(.,'€ /mois')]//text()", input_type="M_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@id,'lightSlider')]//@src", input_type="M_XPATH")
        
        energy_label = response.xpath("//div[contains(@id,'image-dpe')]//@style[contains(.,'dpe')]").get()
        if energy_label:
            energy_label = energy_label.split("dpe-")[1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)
            
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@id,'texte-detail')]//text()").getall())
        if available_date and "Disponible"in available_date:
            available_date = available_date.split("Disponible")[1].replace("Mandat",".").replace("Loyer",".").split(".")[0].replace("le","").strip()
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//div[contains(@class,'champsSPEC-element')][contains(.,'Etage')]//following-sibling::div//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//div[contains(@class,'champsSPEC-element')][contains(.,'Piscine')]//following-sibling::div//text()[contains(.,'Oui')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[contains(@class,'champsSPEC-element')][contains(.,'Parking')]//following-sibling::div//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[contains(@class,'champsSPEC-element')][contains(.,'Balcon')]//following-sibling::div//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[contains(@class,'champsSPEC-element')][contains(.,'Terrasses')]//following-sibling::div//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@class,'agence')]//h2//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@class,'agence')]//text()", input_type="M_XPATH",split_list={"Téléphone :":1,"Fax":0})
        
        mail = "".join(response.xpath("//div[contains(@class,'agence')]//text()").getall())
        if mail:
            if "Le Beausset" in mail:
                item_loader.add_value("landlord_email", "lebeausset@gti-gpi.com")
            elif "Sanary sur Mer" in mail:
                item_loader.add_value("landlord_email", "contact@gti-gpi.com")
            elif "Toulon" in mail:
                item_loader.add_value("landlord_email", "toulon@gti-gpi.com")
        
        yield item_loader.load_item()