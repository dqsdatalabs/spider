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
import re
from bs4 import BeautifulSoup

class MySpider(Spider):
    name = 'cabinethomas_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cabinethomas.fr/resultats?transac=location&type%5B%5D=appartement&budget_maxi=&surface_mini=&ref_bien=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cabinethomas.fr/resultats?transac=location&type%5B%5D=maison&budget_maxi=&surface_mini=&ref_bien=",
                ],
                "property_type" : "house"
            },
        ]

        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    # 1. FOLLOWING
    def parse(self, response):

        data=response.xpath("//script[contains(.,'properties')]/text()").get()
        if data:
            data_j=("[" + data.split("var properties = [")[1].split("];")[0].strip()+ "]")

            list=[]
            current=re.findall(r"(?<=lien)",data_j)
            a=len(current)
            for x in range(a):
                url = self.sub_string_between(data_j,'"lien": "', '",').replace('"', "").strip()
                index=data_j.find("lien")
                data_j=data_j[(index+10):]
                list.append(url)
            for item in list:
                follow_url = response.urljoin(item)
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})



    # # 2. SCRAPING level 2
    def populate_item(self, response):
 
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Cabinethomas_PySpider_france")

        external_id = response.xpath("//h3/span[@class='small']/text()").get()
        if external_id:
            external_id = external_id.split(".")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address =response.xpath("//span[@itemprop='streetAddress']/text()").get()
        if address:
            address=address.upper()
            item_loader.add_value("address", address)

        city=response.xpath("//span[@itemprop='addressLocality']/text()").get()
        if city:
            city=city.upper()
            item_loader.add_value("city", city)
        zipcode=response.xpath("//span[@itemprop='postalCode']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
 
        # desc = " ".join(response.xpath("//div[contains(@class,'desc-text')]//text()").getall())
        # if desc:
        #     desc = re.sub('\s{2,}', ' ', desc.strip())
        #     item_loader.add_value("description", desc)
        
        rent =response.xpath("//span[@class='text-primary-color']/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit =response.xpath("//li[contains(text(),'Dépôt de garantie')]/strong/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].strip()
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li[contains(text(),'Frais')]/strong/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        elevator = response.xpath("//div[@class='panel-body']//ul//li//text()[contains(.,'Ascenseur')]/following-sibling::strong/text()").get()
        if elevator and "Oui" in elevator:
            item_loader.add_value("elevator", True)
        

        # furnished = response.xpath("//span[contains(@class,'alur_location_meuble')]//text()").get()
        # if furnished:
        #     item_loader.add_value("furnished", True)

        room_count =response.xpath("//div[@class='panel-body']//ul//li//text()[contains(.,'pièce(s)')]/following-sibling::strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count =response.xpath("//ul[@class='list-unstyled amenities amenities-detail']//li//i[@class='icons icon-bathroom']/following-sibling::text()").get()
        if bathroom_count:
            bathroom_count=re.findall("\d+",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath("//ul[@class='list-unstyled amenities amenities-detail']//strong[contains(.,'Surface:')]//following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        
        images = [x for x in response.xpath("//li[@class='img-container']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        # latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        # if latitude_longitude:
        #     latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
        #     longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
        #     item_loader.add_value("longitude", longitude)
        #     item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "CABINET THOMAS M  ET J-M")
        item_loader.add_value("landlord_phone", "02.31.85.54.49")
        
        yield item_loader.load_item()
    def sub_string_between(self, source, s1, s2):
        tmp = source[source.index(s1) + len(s1) :]
        return tmp[: tmp.index(s2)]