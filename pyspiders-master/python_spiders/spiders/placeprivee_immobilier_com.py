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
class MySpider(Spider):
    name = 'placeprivee_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.placeprivee-immobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_30_MIN=&C_36_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&keywords=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.placeprivee-immobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=1692662937249640&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_30_MIN=&C_36_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&keywords=",
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

        for item in response.xpath("//div[@class='item-listing']//div[@class='infos-product']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//li[contains(@class,'next-link') and contains(@class,'active')]/a/@href").get()
        if next_page: yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)  
        item_loader.add_value("external_source", "Placeprivee_Immobilier_PySpider_france")    
        item_loader.add_xpath("title", "//h1/text()") 
        external_id = response.xpath("//span[contains(.,'Ref. :')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        room_count = response.xpath("//div[div[.='Chambres']]/div[2]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[div[.='Nombre pièces']]/div[2]//text()")
        bathroom_count = response.xpath("//ul[@class='list-group']//div[div[contains(.,'Salle(s) d')]]/div[2]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        address = response.xpath("//div[@class='product-localisation']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", " ".join(address.strip().split(" ")[1:]))
            item_loader.add_value("zipcode", address.split(" ")[0].strip())
        item_loader.add_xpath("floor","//div[div[.='Etage']]/div[2]//text()")
        furnished = response.xpath("//div[div[.='Meublé']]/div[2]//text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//div[div[.='Ascenseur']]/div[2]/b/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        terrace = response.xpath("//div[div[.='Nombre de terrasses']]/div[2]/b/text()").get()
        if terrace:
            if "0" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        swimming_pool = response.xpath("//div[div[.='Piscine']]/div[2]/b/text()").get()
        if swimming_pool:
            if "non" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)
  
        parking = response.xpath("//div[div[.='Nombre places parking']]/div[2]/b/text()").get()
        if parking:
            if "0" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        square_meters = response.xpath("//div[div[.='Surface']]/div[2]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].split(".")[0])
            
        available_date = response.xpath("//div[div[.='Disponibilité']]/div[2]//text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//div[@class='desc-text']/text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
       
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider_product_vignettes']/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
      
        rent = response.xpath("//span[contains(@class,'alur_loyer_price')]//text()").get()
        if rent:
            rent = rent.split("Loyer")[1].split("€")[0].split(".")[0].strip().replace(" ","").replace('\xa0', '')
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        utilities = response.xpath("//div[div[.='Provision sur charges']]/div[2]//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//div[div[.='Dépôt de Garantie']]/div[2]//text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit)
        energy_label = response.xpath("//div[div[.='Conso Energ']]/div[2]//text()[.!='Vierge']").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("landlord_name", "PLACE PRIVEE")
        item_loader.add_value("landlord_phone", "03.83.20.22.77")
        
        yield item_loader.load_item()