# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..user_agents import random_user_agent
import re
from ..helper import extract_number_only
import lxml
import js2xml
from scrapy import Selector
from scrapy import Request,FormRequest

class LeCannetDesMauresGuyHoquetComSpider(scrapy.Spider):
    name = "le_cannet_des_maures_guy_hoquet_com"
    allowed_domains = ["le-cannet-des-maures.guy-hoquet.com","www.limmobiliere-guignard.com"]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=','
    scale_separator='.'
    position = 0
    external_source="LeCannetDesMauresGuyHoquet_PySpider_france_fr"
    start_urls = "https://www.limmobiliere-guignard.com/fr/locations/1"
    # custom_settings = {
    #     "PROXY_ON" : "True"
    # }
    headers = {
        ':method': 'GET',
        ':scheme': 'https',
        'cache-control': 'max-age=0',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36'
    }
    def start_requests(self):
        yield Request(url=self.start_urls,
                        headers=self.headers,
                        callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//article[@class='fiches fiches-immo']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, headers=self.headers, callback=self.get_property_details)
            seen=True
        
        if page ==2 or seen:        
            f_url = response.url.replace(f"locations/{page-1}", f"locations/{page}")
            yield Request(f_url, callback=self.parse, headers=self.headers, meta={"page": page+1})

    
    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)

     
  
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        desc=response.xpath("//h2[.='Descriptif détaillé']/following-sibling::div/text()").get()
        if desc:
            item_loader.add_value("description",desc)

        external_id = response.xpath('.//p[contains(.,"Réf")]/text()').get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(' : ')[-1])

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        rent= response.xpath("//span[@class='prix has_sup']/text()").get() 
        if rent:
            rent=rent.split(":")[-1].split("€")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency","GBP")
        deposit=response.xpath("//li[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            deposit=deposit.split(":")[-1].replace("€","").strip()
            item_loader.add_value("deposit",deposit)
        square_meters=response.xpath("//li[contains(.,'Surface habitable')]/text()").get()
        if square_meters:
            square=square_meters.split(":")[-1].split(".")[0].strip()
            item_loader.add_value("square_meters",square)
        bathroom_count=response.xpath("//li[contains(.,'Salle d')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(":")[-1])
        room_count=response.xpath("//li[contains(.,'Nbre de chambres')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1])
        parking=response.xpath("//p[contains(.,'parking')]/text()").getall()
        if parking:
            item_loader.add_value("parking",True)


        javascript = response.xpath('.//script[contains(text(), "latLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            # print(xml)
            item_loader.add_value('latitude', xml_selector.xpath('.//var[@name="position"]/array/number/@value').extract_first())
            item_loader.add_value('longitude', xml_selector.xpath('.//var[@name="position"]/array/number/@value').extract()[1])

        javascript = response.xpath('.//script[contains(text(), "postalCode")]/text()').extract_first()
        if javascript:
            city = re.findall(r'\"addressLocality\"\:\"(.*)?\"',javascript)
            if city:
                item_loader.add_value('city', city[0])
            zipcode = re.findall(r'\"postalCode\"\:\"(.*)?\"',javascript)
            if zipcode: 
                item_loader.add_value('zipcode', zipcode[0])
        adres=item_loader.get_output_value("city")+" "+item_loader.get_output_value("zipcode")
        if adres:
            item_loader.add_value("address",adres)
        
        furnished = response.xpath("//li[contains(.,'Meublé')]/text()").get()
        if furnished: 
            if furnished.split(":")[-1].strip().lower() == "oui": item_loader.add_value("furnished", True)
            elif furnished.split(":")[-1].strip().lower() == "non": item_loader.add_value("furnished", False)

        images=response.xpath("//div[@class='bloc_img']/a/img/@src").get()
        if images:
            item_loader.add_value('images',images)
      

        desc = item_loader.get_collected_values("description")
        if get_p_type_string(str(desc)):
            item_loader.add_value("property_type", get_p_type_string(str(desc)))
        else:
            if get_p_type_string(response.url): item_loader.add_value("property_type", get_p_type_string(response.url))
            else: return

        # utilities = response.xpath("//div[@class='blocAlur']/text()[2]").get()
        # if utilities:
        #     item_loader.add_value("utilities", utilities.split("EUR")[0].strip().split(" ")[-1])
        
        item_loader.add_value("landlord_name", 'Limmobiliere')
        item_loader.add_value("landlord_phone", "02 54 08 07 06")
        
        item_loader.add_value("external_source", self.external_source)
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None