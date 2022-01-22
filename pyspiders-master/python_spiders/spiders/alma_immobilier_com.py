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
    name = 'alma_immobilier_com'
    execution_type='testing' 
    country='france'
    locale='fr'
    custom_settings = {
        "PROXY_ON" : True,
        
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.alma-immobilier.com/rechercher-bien/?location=france&status=location&type%5B%5D=appartement-a-louer-vide&type%5B%5D=appartement-a-louer-meuble&type%5B%5D=appartement-a-louer",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.alma-immobilier.com/rechercher-bien/?location=france&status=location&type%5B%5D=maison-a-louer-vide&type%5B%5D=maison-a-louer-meuble",
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
        for item in response.xpath("//div[@class='span6 ']//h4/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[contains(.,'Suivant')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Alma_Immobilier_PySpider_"+ self.country)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//h1/span/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        address = response.xpath("//div[@class='page-breadcrumbs']//ul/li[3]//text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        zipcode = response.xpath("//div[@class='page-breadcrumbs']//ul/li[4]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        square_meters = response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'M2') or contains(.,'m2') or contains(.,'m²')  or contains(.,'M²')]/text()").get()
        if square_meters:
            square_meters = square_meters.replace("\xa0"," ").strip().split(" ")[0]
            item_loader.add_value("square_meters", square_meters)
        else:
            square_meters = response.xpath("//div[@class='property-meta clearfix']/span[1]/text()").get()
            if square_meters and square_meters.strip().isdigit():
                item_loader.add_value("square_meters", square_meters.strip())
        
        room_count = response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'Chambre')]/text()").get()
        if room_count:
            room_count = room_count.replace("\xa0"," ").strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'Pièce')]/text()").get()
            room_count = room_count.replace("\xa0"," ").strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'Salle')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.replace("\xa0"," ").strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//h5[@class='price']/span[contains(.,'€')]/text()").get()
        if rent:
            price = rent.split("/")[0].split("€")[1].strip().replace("\xa0","").split(" ")[0]
            item_loader.add_value("rent", price)
            if int(price.replace(".",""))>9999:
                print(f"{price}------->{response.url}")
        
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("//h4[@class='title'][contains(.,'RÉFÉRENCE')]/text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)
        idcheck=item_loader.get_output_value("external_id")
        if not idcheck:
            id=response.url.split("property/")[-1]
            item_loader.add_value("external_id",id.replace("/",""))
        
        desc = "".join(response.xpath("//div[contains(@class,'content')]/p//text() | //div[contains(@dir,'ltr')]//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        else:
            desc = "".join(response.xpath("//div[@dir='ltr']/div[@dir='ltr']//text()").getall())
            if desc:
                item_loader.add_value("description", desc)
            else:
                desc = "".join(response.xpath("//div[@class='column']/p//text() | //div[contains(@class,'content clearfix')]/div//text()").getall())
                if desc:
                    item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//ul[@class='slides']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        imagescheck=item_loader.get_output_value("images")
        if not imagescheck:
            images=response.xpath("//meta[@name='twitter:image']/@content").get()
            if images:
                item_loader.add_value("images",images)
        lat_lng = response.xpath("//script[contains(.,'propertyMarkerInfo')]/text()").get()
        if lat_lng:
            latitude = lat_lng.split('"lat":"')[1].split('"')[0]
            longitude = lat_lng.split('"lang":"')[1].split('"')[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        floor = response.xpath("//div[@class='features']/ul//li[contains(.,'étage')]//text()").get()
        floor2 = response.xpath("//div[@class='features']/ul//li[contains(.,'Etage')]//text()").get()
        if floor:
            floor = floor.strip().split(" ")[0].replace("er","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        elif floor2:
            floor2 = floor2.split(":")[1].strip().split(" ")[0].replace("eme","")
            item_loader.add_value("floor", floor2)
            
        utilities = response.xpath("//div[@class='common-note']/p[contains(.,'charge') and not(contains(.,'Loyer'))]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].strip())
        
        furnished = response.xpath("//div[@class='features']/ul//li[contains(.,' meuble') or contains(.,'Meuble')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//div[@class='features']/ul//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//div[@class='features']/ul//li[contains(.,'terrasse') or contains(.,'Terrasse')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//div[@class='features']/ul//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//div[@class='features']/ul//li[contains(.,'Ascenseur')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        item_loader.add_value("landlord_name", "PIGUET ALMA IMMOBILIER")
        
        phone = "".join(response.xpath("//li[@class='office']/text()").getall())
        if phone:
            item_loader.add_value("landlord_phone", phone.split(":")[1].strip())
        phonecheck=item_loader.get_output_value("landlord_phone")
        if not phonecheck:
            item_loader.add_value("landlord_phone","+33 (0)1 40 70 95 10")
        
        item_loader.add_value("landlord_email", "contact@alma-immobilier.fr")

        
        yield item_loader.load_item()