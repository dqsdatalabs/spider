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
    name = 'promovente_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Promovente_PySpider_france'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.promovente.com/louer?search=1&transaction=2&type%5B%5D=3&prixmin=&prixmax=&quartier=&ref=&order=3",
                    "http://www.promovente.com/louer?search=1&transaction=2&type%5B%5D=4&prixmin=&prixmax=&quartier=&ref=&order=3",
                    "http://www.promovente.com/louer?search=1&transaction=2&type%5B%5D=5&prixmin=&prixmax=&quartier=&ref=&order=3",
                    "http://www.promovente.com/louer?search=1&transaction=2&type%5B%5D=6&prixmin=&prixmax=&quartier=&ref=&order=3",
                    "http://www.promovente.com/louer?search=1&transaction=2&type%5B%5D=7&prixmin=&prixmax=&quartier=&ref=&order=3",

                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "http://www.promovente.com/louer?search=1&transaction=2&type%5B%5D=1&prixmin=&prixmax=&quartier=&ref=&order=3",
                    ],
                "property_type": "house"
            },
            {
                "url": [
                    "http://www.promovente.com/louer?search=1&transaction=2&type%5B%5D=2&prixmin=&prixmax=&quartier=&ref=&order=3",
                    ],
                "property_type": "studio"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='bien-card slider-item']//h2"):
            url = item.xpath("./a/@href").get()
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//div[@class='pagination']/a[position()<2]/@href").get()   
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Promovente_PySpider_france")
        item_loader.add_xpath("title", "//title/text()")   
        item_loader.add_xpath("external_id", "substring-after(//span[@class='reference']/text(),': ')")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        rent= response.xpath("//span[@class='price ti-bold tx-cyan']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        item_loader.add_xpath("utilities", "substring-after(//span//text()[contains(.,'Charges :')],': ')")
        item_loader.add_xpath("deposit", "substring-after(//span//text()[contains(.,'garantie :')],': ')")
        
        square_meters=response.xpath("//li[span[.='Surface habitable']]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())
        
        room_count = response.xpath("//li[span[contains(.,'Chambre')]]/strong/text()").get()
        if room_count: 
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[span[contains(.,'Pièce')]]/strong/text()")

        city = response.xpath("//div[@class='location']//text()").get()
        if city:
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address", city.strip())

        bathroom_count= response.xpath("//li[span[contains(.,'Salle de ')]]/strong/text()[.!='0']").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        desc=" ".join(response.xpath("//div[@class='desc']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//div[@class='thumb ']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name","Promovente")
        item_loader.add_value("landlord_phone","03 20 31 08 76")
        item_loader.add_value("landlord_email","contact@promovente.com")
        
        parking = response.xpath("//li[span[.='Garage' or .='Parking']]/strong/text()").get()
        if parking:
            if "oui" in parking.lower():
                item_loader.add_value("parking",True)
            elif "non" in parking.lower():
                item_loader.add_value("parking",False)
            else:
                item_loader.add_value("parking",True)

        terrace = response.xpath("//li[span[.='Terrasse']]/strong/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace",False)
            else:
                item_loader.add_value("terrace",True)
        furnished = response.xpath("//li[span[.='Garage' or .='Parking']]/strong/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished",False)
            else:
                item_loader.add_value("furnished",True)
        yield item_loader.load_item()