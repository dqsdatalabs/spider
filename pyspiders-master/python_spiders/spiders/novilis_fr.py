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

class MySpider(Spider):
    name = 'novilis_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Novilis_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.novilis.fr/louer/appartement?toFeatures=true&search%5Bprice%5D%5Bmin%5D=0.00&search%5Bprice%5D%5Bmax%5D=3000.00&search%5Brooms%5D%5Bmin%5D=0.00&search%5Brooms%5D%5Bmax%5D=6.00&search%5Barea%5D%5Bmin%5D=0.00&search%5Barea%5D%5Bmax%5D=300.00&_token=unjhnnEJrzcWxoz6KQLjlsbnEa2ym9DQuq0mgdq0", "property_type": "apartment"},
            {"url": "https://www.novilis.fr/louer/maison?toFeatures=true&search%5Bprice%5D%5Bmin%5D=0.00&search%5Bprice%5D%5Bmax%5D=3000.00&search%5Brooms%5D%5Bmin%5D=0.00&search%5Brooms%5D%5Bmax%5D=6.00&search%5Barea%5D%5Bmin%5D=0.00&search%5Barea%5D%5Bmax%5D=300.00&_token=unjhnnEJrzcWxoz6KQLjlsbnEa2ym9DQuq0mgdq0", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            follow_url = item["properties"]["url"]
            lng, lat = item["geometry"]["coordinates"]
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), "lat":str(lat), "lng":str(lng)})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        external_id = response.url
        if external_id:
            external_id = external_id.split('louer/')[-1].split('/',1)[-1].split('-')[0].strip()
            item_loader.add_value("external_id", external_id)
        item_loader.add_value("latitude", response.meta.get('lat'))
        item_loader.add_value("longitude", response.meta.get('lng'))

        rent =  "".join(response.xpath("//span[@class='price']/text()").extract())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))

        city = response.xpath("//h1/text()").get()
        if city:
            city = city.split("-")[-1].split("(")[0].strip()
            item_loader.add_value("city", city)
  
        deposit = response.xpath("substring-after(//ul[@class='rental-informations']/li[contains(.,'Dépôt de garantie')]/text(),':')").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip().replace(" ","").rstrip("€"))
        
        utilities = response.xpath("substring-after(//ul[@class='rental-informations']/li[contains(.,'sur charges')]/text(),':')").get()
        if utilities:
            item_loader.add_value("utilities", utilities.strip().replace(" ","").rstrip("€"))
  
        meters =  "".join(response.xpath("//div[@class='features-left']/p[contains(.,'Surface : ')]/strong/text()").extract())
        if meters:
            s_meters = meters.split("m²")[0]
            item_loader.add_value("square_meters", int(float(s_meters)))
        
        room_count =  "".join(response.xpath("//div[@class='features-left']/p[contains(.,'Type : ')]/strong/text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.replace("T",""))

        address =  ".".join(response.xpath("//div[contains(@class,'content-header')]/h1/text()").extract())
        if address:
            item_loader.add_value("address", address.split("-")[1].strip())
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])

        desc = "".join(response.xpath("//div[contains(@class,'description-bloc')]/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        available_date = response.xpath("//div[@class='features-left']/p[contains(.,'Disponibilité : ')]/strong/text()[(contains(.,'/'))]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@id='container-mosaic']/div//a//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone", "05 61 74 12 36")
        item_loader.add_value("landlord_name", "Novil")
        item_loader.add_value("landlord_email", "novilis@novilis.fr")
        yield item_loader.load_item()