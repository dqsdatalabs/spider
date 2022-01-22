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
    name = 'agencealbertinice_com'
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
            yield FormRequest(url="http://www.agencealbertinice.com/fr/recherche/",
                            callback=self.parse,
                            formdata=self.form_data,
                            dont_filter=True,
                            meta={
                                'property_type': url.get('property_type'),
                                "type":url.get('type')
                            })

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//ul[contains(@class,'ads')]//div//a[@class='button']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_id", response.url.split("-")[-1])

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Agencealbertinice_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@id,'description')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface ')]//span//text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        if response.xpath("//li[contains(.,'Chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Chambre')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        if response.xpath("//li[contains(.,'Pièce')]//span//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Pièce')]//span//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(@class,'alt')][contains(.,'Salle')]/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[contains(.,'Mois')]//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Disponible le')]//span//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Dépôt de garantie')]//span//text()", input_type="F_XPATH", get_num=True, split_list={"€":0}, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'owl-carousel')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'marker')]//text()", input_type="F_XPATH", split_list={"marker_map_2 =":1,"marker([":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'marker')]//text()", input_type="F_XPATH", split_list={"marker_map_2 =":1,"marker([":1,",":1,"]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[contains(.,'Etage')]//span//text()", input_type="F_XPATH", split_list={"/":0})
        # ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(@class,'alt')][contains(.,'Charges')]//span//text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[contains(@class,'services')]//li[contains(.,'Ascenseur')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[contains(@class,'services')]//li[contains(.,'Meublé')]/text()", input_type="F_XPATH", tf_item=True)
        
        address = "".join(response.xpath("//span[contains(@class,'selectionLink')]//following-sibling::h2//text()").getall())
        if address:
            address = address.replace("Appartement","").replace("Studio","").strip().split(" ")[0]
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        utilities = "".join(response.xpath("//p[@id='description']/text()[contains(.,'location')]").getall())
        if utilities:
            if ":" in utilities:
                utilities = utilities.split(":")[1].replace("€","")
            item_loader.add_value("utilities", utilities)

        bathroom_count = "".join(response.xpath("//article//ul/li/text()[contains(.,'salle de')]").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        

        energy_label = response.xpath("//img[contains(@alt,'Énergie - Consommation conventionnelle')]//@src").get()
        if energy_label:
            energy_label = energy_label.split("/")[-1].split("%")[0]
            if int(energy_label)>0:
                item_loader.add_value("energy_label", energy_label)
            
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agence Alberti", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="+33 (0)4 93 27 98 03", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="gaelle@agencealbertinice.com", input_type="VALUE")

        yield item_loader.load_item()