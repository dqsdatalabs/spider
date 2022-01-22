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
    name = 'italianaimmobiliare'
    external_source = "Italianaimmobiliare_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it'
    start_urls = ['https://www.italianaimmobiliare.it/index.php']  # LEVEL 1

    def start_requests(self):
        
        yield Request(
            url=self.start_urls[0],
            callback=self.jump,
        )
    
    def jump(self, response):
        for i in response.xpath("//div[@id='estateTotZone']//a/@href").getall():
            try:
                slug_id = i.split("[ISTAT]=")[1].split("&")[0]
                city = i.split("=")[-1]
                f_url = f"https://www.itimgest.com/api/web_estates.php?filters[e.status]=0&filters[equal][e.parent_estate_id]=0&filters[limit]=120&filters%5Barea_type_and_id%5D%5B0%5D%5BISTAT%5D={slug_id}&filters%5Barea_type_and_id%5D%5B0%5D%5Barea_id%5D=Tutte&filters%5Barea_type_and_id%5D%5B0%5D%5Barea_type%5D=location&filters%5Bbetween%5D%5BMQSuperficie%5D%5Bfrom%5D=20&filters%5Bbetween%5D%5BMQSuperficie%5D%5Bto%5D=&filters%5Bbetween%5D%5BNrLocali%5D%5Bfrom%5D=1&filters%5Bbetween%5D%5BNrLocali%5D%5Bto%5D=&filters%5Bbetween%5D%5BPrezzo%5D%5Bfrom%5D=1&filters%5Bbetween%5D%5BPrezzo%5D%5Bto%5D=&filters%5Bequal%5D%5BContratto%5D=A&filters%5Bequal%5D%5Be.agency_id%5D=&filters%5Bin%5D%5BIDTipologia%5D%5B%5D=&w_tipologia_label=Tutte+le+Tipologie&w_zona_label={city}&zona="
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
                    'Accept': '*/*',
                    'Origin': 'https://www.italianaimmobiliare.it',
                    'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
                }
                
                yield Request(f_url, callback=self.parse, headers=headers)
            except:
                pass
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        
        for item in data:
            follow_url = f"https://www.italianaimmobiliare.it/product_page.php?estate_id={item['estate_id']}"
            yield Request(
                follow_url, 
                callback=self.populate_item, 
                meta={"data": item}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        data = response.meta.get('data')

        item_loader.add_value("external_link", response.url)
        if get_p_type_string(data["NomeTipologia"]):
            item_loader.add_value("property_type", get_p_type_string(data["NomeTipologia"]))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("external_id", data["ref"])
        item_loader.add_value("title", data["estate_title"])
        item_loader.add_value("address", data['area_name'])
        item_loader.add_value("city", data["administrative_area_level_3"])
        item_loader.add_value("zipcode", data['postal_code'])
        
        item_loader.add_value("rent", data["Prezzo"])
        item_loader.add_value("currency", "EUR")
        
        item_loader.add_value("square_meters", data["MQSuperficie"])
        item_loader.add_value("room_count", data["NrLocali"])
        
        bathroom_count = response.xpath("//ul[@class='property-main-features']//img[contains(@src,'bath')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        item_loader.add_value("description", data["estate_description"])
        item_loader.add_value("latitude", data['lat'])
        item_loader.add_value("longitude", data['lng'])

        energy_label = response.xpath("//li[contains(.,'Energetica')]/span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        parking = response.xpath("//img[contains(@src,'garage')]/following-sibling::span/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        
        images = [x.strip("'") for x in response.xpath("//div[contains(@id,'estate_gallery')]//@data-background-image").getall()]
        item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", data["agency_name"])
        item_loader.add_value("landlord_phone", data["agency_phone"])
        item_loader.add_value("landlord_email", data["agency_email"])
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "condiviso" in p_type_string.lower():
        return "room"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartament" in p_type_string.lower() or "flat" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("casa" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None