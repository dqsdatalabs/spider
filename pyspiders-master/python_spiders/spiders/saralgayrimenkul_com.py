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
    name = 'saralgayrimenkul_com'
    execution_type='testing'
    country='turkey'
    locale='tr'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://saralgayrimenkul.com/ara/?cat_id=552",
                "property_type" : "house"
            },
            {
                "url" : "https://saralgayrimenkul.com/ara/?cat_id=555",
                "property_type" : "house"
            },
            {
                "url" : "https://saralgayrimenkul.com/ara/?cat_id=549",
                "property_type" : "apartment"
            },
            {
                "url" : "https://saralgayrimenkul.com/ara/?cat_id=551",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='ads-list-archive']//h3/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
        next_page = response.xpath("//a[contains(.,'»')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Saralgayrimenkul_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//div[@class='descs-box']/h1/text()")

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "//div[@class='row']/div/div/strong[contains(., 'İlan NO')]//following-sibling::span/text()")
        
        address="".join(response.xpath("//div[@class='country-locations']//div[@id='word-count']//text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            try:
                city = address.split(",")[-1]
                item_loader.add_value("city", city.strip())
            except:
                pass

        room_count=response.xpath("//div[@class='row']/div/div/strong[contains(., 'Oda Sayısı')]//following-sibling::span/text()").extract_first()
        if room_count:
            room_count=room_count.split("+")
            item_loader.add_value("room_count", str(int(room_count[0])+int(room_count[1])))
            
        square_meters=response.xpath("//div[@class='row']/div/div/strong[contains(., 'm²')]//following-sibling::span/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        room_bath=response.xpath("//div[@class='row']/div/div/strong[contains(., 'Banyo')]//following-sibling::span/text()").extract_first()
        if room_bath:
            item_loader.add_value("bathroom_count", room_bath)    

        rent = response.xpath("//div[@class='descs-box']/div/h3/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.split(".")[0])
            
        floor=response.xpath("//div[@class='row']/div/div/strong[contains(., 'Bulunduğu Kat')]//following-sibling::span/text()[not(contains(.,'Villa')) and not(contains(.,'K')) and not(contains(.,'Müstakil')) and not(contains(.,'Zemin'))]").extract_first()
        if floor:
            item_loader.add_value("floor", floor)
            
        balcony = response.xpath("//div[@class='row']/div/div/strong[contains(., 'Balkon')]//following-sibling::span/text()").extract_first()
        if balcony:
            if "var" in balcony.lower():
                item_loader.add_value("balcony", True)
            if "yok" in balcony.lower():
                item_loader.add_value("balcony", False)
        utilities=response.xpath("//div[@class='row']/div/div/strong[contains(., 'Aidat')]//following-sibling::span/text()").extract_first()
        if utilities!='*':
            item_loader.add_value("utilities", utilities)
            
        furnished=response.xpath("//div[@class='row']/div/div/strong[contains(., 'Eşyalı')]//following-sibling::span/text()").extract_first()
        if furnished:
            if "Evet" in furnished:
                item_loader.add_value("furnished", True)
            elif "Hayır" in furnished:
                item_loader.add_value("furnished", False) 
            
        desc="".join(response.xpath("//div[@class='desc-points']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//div[@class='flexslider single-page-slider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

            
        elevator=response.xpath("//div[@class='ilandetay'][contains(., 'Asansör')]//text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)

        parking=response.xpath("//div[@class='ilandetay'][contains(., 'Otopark')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        feature = response.xpath("//div[@class='ilandetay'][contains(., 'Özellikler')]//span/text()").extract_first()
        if feature:
            if "teras" in feature.lower():
                item_loader.add_value("terrace", True)
            if "balkon" in feature.lower():
                item_loader.add_value("balcony", True)
            if "bulaşık makine" in feature.lower():
                item_loader.add_value("dishwasher", True)
            if "çamaşır makine" in feature.lower():
                item_loader.add_value("washing_machine", True)
            
        swimming_pool=response.xpath("//div[@class='ilandetay'][contains(., 'Yüzme Havuzu')]//text()").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
            
        item_loader.add_value("landlord_name", "Saral Gayrimenkul")
        item_loader.add_value("landlord_phone", "444 6 773")
        item_loader.add_value("landlord_email","info@saralgayrimenkul.com")

        latitude =response.xpath("//input[@id='lat']/@value").extract_first()
        longitude =response.xpath("//input[@id='lon']/@value").extract_first()
        if latitude and longitude:
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        yield item_loader.load_item()