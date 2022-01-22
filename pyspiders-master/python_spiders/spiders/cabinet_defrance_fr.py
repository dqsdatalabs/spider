# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re
import dateparser
class MySpider(Spider):
    name = 'cabinet_defrance_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.cabinet-defrance.com/moteur,prevalidation.htm?idqfix=1&idtt=1&idtypebien=1&saisie=O%C3%B9+d%C3%A9sirez-vous+habiter+%3F&idq=&div=&idpays=&cp=&ci=&tri=d_dt_crea&annlistepg={}",
                ],
                "property_type": "apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item.format(1),
                    callback=self.parse,
                    meta={'property_type': url.get('property_type'), "base":item}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='span8']/a"):
            seen = True
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )
        if page == 2 or seen:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type'), "page":page+1, "base":base}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1].split(".")[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        available_date = response.xpath("//p[@itemprop='description']//text()[contains(.,'DISPONIBLE LE')]").extract_first() 
        if available_date:  
            date_parsed = dateparser.parse(available_date.split("DISPONIBLE LE")[1].strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d")) 
        else:
            available_date = response.xpath("//p[@itemprop='description']//text()[contains(.,'DISPONIBLE le')]").extract_first() 
            if available_date:  
                date_parsed = dateparser.parse(available_date.split("DISPONIBLE le")[1].strip(),date_formats=["%d-%m-%Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d")) 
        room_count = response.xpath("//li[div[.='Chambres' or .='Chambre']]/div[2]/text()").get()
        if room_count:    
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[div[.='Pièces' or .='Pièce']]/div[2]/text()")
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value='//li[div[.="Salle de bain" or .="Salle d\'eau"]]/div[2]/text()', input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Cabinet_Defrance_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//li[div[.='Etage']]/div[2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()[last()]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1//text()[last()]", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1//text()[last()]", input_type="F_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[p='Consommations énergétiques']//div[@class='row-fluid']/div[contains(@class,'dpe-bloc-lettre')]/text()[not(contains(.,'V'))]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[@itemprop='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[div[.='Surface']]/div[2]/text()", input_type="F_XPATH", get_num=True, split_list={",":0,"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'recherche-annonces-prix')]//span[@itemprop='price']/text()", input_type="F_XPATH", get_num=True, replace_list={" ":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//strong[contains(.,'Dépôt de garantie :')]/text()", input_type="F_XPATH", get_num=True, split_list={":":-1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li[contains(.,'Charges :')]/text()", input_type="F_XPATH", get_num=True, split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='slider']/a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[div[.='Parking']]/div[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[div[.='Meublé']]/div[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[div[.='Ascenseur']]/div[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[div[.='Balcon']]/div[2]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CABINET DEFRANCE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01 43 28 09 00", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="cabinet-defrance@wanadoo.fr", input_type="VALUE")

        
        yield item_loader.load_item()