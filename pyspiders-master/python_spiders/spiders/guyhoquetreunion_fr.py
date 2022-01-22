# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, re


class MySpider(Spider):
    name = 'guyhoquetreunion_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Guyhoquet_Reunion_PySpider_france"
    # start_urls = ["https://www.guyhoquet-reunion.fr/annonces/transaction/Location.html"] 
    
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.guyhoquet-reunion.fr/catalog/advanced_search_result.php?action=update_search&search_id=1709691379313745&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                "property_type" : "apartment",
            }, 
            {
                "url" : "https://www.guyhoquet-reunion.fr/catalog/advanced_search_result.php?action=update_search&search_id=1709691379313745&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                "property_type" : "house",
            },

        ]
         
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        prop = response.meta.get('property_type')
        for item in response.xpath("//a[@class='link-product']/@href").getall():
            follow_url=response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":prop})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)


        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//span[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())

        address=response.xpath("//h1[@class='product-title']/span/text()").get()
        if address:
            item_loader.add_value("address",address)
        city=response.xpath("//h1[@class='product-title']/span/text()").get()
        if city:
            item_loader.add_value("city",city.split(" ")[0])
        zipcode=response.xpath("//h1[@class='product-title']/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(" ")[-1])
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='products-description']/text()").getall()).strip()   
        if description:
            description=description.replace("\n","").replace("\t","")
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//div[.='Surface']/following-sibling::div/b/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split(".")[0])

        room_count = response.xpath("//div[.='Chambres']/following-sibling::div/b/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        
        bathroom_count = response.xpath("//div[.='Salle(s) de bains']/following-sibling::div/b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].replace("\xa0","").split(" ")[-1])
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//div[.='Disponibilité']/following-sibling::div/b/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['fr'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//div[.='Dépôt de Garantie']/following-sibling::div/b/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(" ")[0])
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='item-slider']//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//script[contains(.,'google.maps.LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('init_map_product')[-1].split("myOptions")[0].split('google.maps.LatLng')[-1].split(',')[0].strip().replace("(-",""))
            item_loader.add_value("longitude", latitude.split('init_map_product')[-1].split("myOptions")[0].split('google.maps.LatLng')[-1].split(',')[-1].split(")")[0].strip())

        
        parking = response.xpath("//div[.='Nombre places parking']/following-sibling::div/b/text()").get()
        if parking:
            item_loader.add_value("parking",True)

        terrace = response.xpath("//div[.='Nombre de terrasses']/following-sibling::div/b/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)

        furnished = response.xpath("//div[.='Meublé']/following-sibling::div/b/text()").get()
        if furnished:
            if furnished.strip().lower() == 'oui':
                item_loader.add_value("furnished", True)
            elif furnished.strip().lower() == 'non':
                item_loader.add_value("furnished", False)


        item_loader.add_value("landlord_name", "GUY HOQUET RÉUNİON")
        item_loader.add_value("landlord_phone", "02 62 32 13 21")
        item_loader.add_value("landlord_email", "referencementprestataire@gmail.com")

        yield item_loader.load_item()