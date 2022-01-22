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
    name = 'lozano_gestion_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lozano-gestion.com/nos-annonces/?natures=204&saisonnier=0&numero_page=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.lozano-gestion.com/nos-annonces/?natures=205&saisonnier=0&numero_page=1",
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

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[@class='voirAnnonce']/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        if page == 2 or seen:
            follow_url = response.url.replace("&numero_page=" + str(page - 1), "&numero_page=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Lozano_Gestion_PySpider_france", input_type="VALUE")
        
        address = response.xpath("//td[contains(.,'Adresse')]/following-sibling::td/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1].strip().split(" ")[0]
            if zipcode.isdigit():
                item_loader.add_value("zipcode", zipcode)
                city = address.split(zipcode)[1].strip()
                item_loader.add_value("city", city.replace("6EME",""))
        
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1[@itemprop='name']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//p[contains(.,'Surface')]/span/text()", input_type="F_XPATH", get_num=True, split_list={"m":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[contains(.,'Chambre')]/span/text() | //p[contains(.,'Pièce')]/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[contains(.,'Salle')]/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//td[contains(.,'Loyer mensuel toutes')]/following-sibling::td/span[1]/text()", input_type="F_XPATH", get_num=True, replace_list={"€":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//td[contains(.,'Disponibilité')]/following-sibling::td/text()", input_type="F_XPATH", replace_list={"le":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//td[contains(.,'Dépôt')]/following-sibling::td/text()", input_type="F_XPATH", get_num=True, replace_list={"€":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//td[contains(.,'Référence')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-inner']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lat":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={'lng":"':1, '"':0})
        
        energy_label = response.xpath("//div[@class='class_conteneur']//img[contains(@src,'dpe_energie')]/parent::div/div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//p[contains(.,'Etage')]/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//td[contains(.,'Charges')]/following-sibling::td/text()", input_type="F_XPATH", get_num=True, replace_list={"€":""})
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//p[contains(.,'Ascenseur')]/span/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Régie Gindre & Lozano", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 78 57 13 89", input_type="VALUE")

        yield item_loader.load_item()