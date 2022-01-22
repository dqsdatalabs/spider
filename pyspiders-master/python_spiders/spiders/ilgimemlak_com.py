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
    name = 'ilgimemlak_com'
    start_urls = ['http://www.ilgimemlak.com/emlak/eRNA3xMId-8j-K-k.aDZ43ep7I1heLnbCJIq-P3woC1xns0uR1AnIhKcJfJ2Dqlf&new=1']  # LEVEL 1
    execution_type='testing'
    country='turkey'
    locale='tr'
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@id='content_uppList']/div[@class='item']"):
            follow_url = response.urljoin(item.xpath(".//a[@class='title']/@href").get())
            property_type = item.xpath(".//span[contains(@id,'content_rptRealtyList_lblRealtyCategoryType')]/text()").get()
            if property_type and "Residence" in property_type:
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={"prop_type":property_type})
            elif property_type and "Daire" in property_type:
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={"prop_type":property_type})
            seen = True
        
        if page == 2 or seen:
            url = f"http://www.ilgimemlak.com/emlak/eRNA3xMId-8j-K-k.aDZ43ep7I1heLnbCJIq-P3woC1xns0uR1AnIhKcJfJ2Dqlf&new=1&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Ilgimemlak_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div[@class='title']/h1/span/text()").get()
        item_loader.add_value("title",title)
        
        item_loader.add_value("external_link", response.url)
        
        item_loader.add_xpath("external_id", "//dl[@class='realty-details']/dd[1]/text()")
        
        property_type = response.meta.get("prop_type")
        if property_type:
            item_loader.add_value("property_type", property_type)
        else:
            return
        
        room=response.xpath("//dl/div/dt[span[.='Oda Sayısı']]//following-sibling::dd/text()").extract_first()
        if room:
            room=room.split()
            item_loader.add_value("room_count", str(int(room[0])+ int(room[2])))
        elif response.xpath("//dl/div/dt[span[.='Bölüm Sayısı']]//following-sibling::dd/text()"):
            item_loader.add_xpath("room_count", "//dl/div/dt[span[.='Bölüm Sayısı']]//following-sibling::dd/text()")
        
        bathroom=response.xpath("//dl/div/dt[span[.='Banyo Sayısı']]//following-sibling::dd/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())
        
        city=response.xpath("//h2/span[3]/text()").extract_first()
        district=response.xpath("//h2/span[4]/text()").extract_first()
        if city or district:
            item_loader.add_value("address", city+" "+district)
            item_loader.add_value("city", city)
        
        square_meters=response.xpath("//dl/div/dt[span[.='Metrekare']]//following-sibling::dd/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split())
        
        rent=response.xpath("//div[@class='title']/h3/span/text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.split("TL")[0].strip())
            item_loader.add_value("currency", "TRY")
        
        utilities=response.xpath("//dl/div/dt[span[.='Aidat']]//following-sibling::dd/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("TL")[0].strip())
        deposit=response.xpath("//dl/div/dt[span[.='Depozito']]//following-sibling::dd/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("TL")[0].strip())
        
        floor=response.xpath("//dl/div/dt[span[.='Bulunduğu Kat']]//following-sibling::dd/text()").extract_first()
        if floor:
            item_loader.add_value("floor", floor.split())
            
        desc="".join(response.xpath("//span[@id='content_ctlRealtyDescription1_lblRealtyDescription']//text()").extract())
        if desc:
            item_loader.add_value("description", desc)
        
        images=[x for x in response.xpath("//div[@class='photo-gallery']/div/ul/li/a/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        balcony=response.xpath("//div[@class='c']/ul/li/span[.='Balkon']/text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator=response.xpath("//div[@class='c']/ul/li/span[.='Asansör']/text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished=response.xpath("//div[@class='c']/ul/li/span[.='Mobilyalı']/text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
        elif "eşyalı" in title.lower():
            item_loader.add_value("furnished", True)
            
        wash=response.xpath("//div[@class='c']/ul/li/span[.='Beyaz Eşyalı']/text()").extract_first()
        if wash:
            item_loader.add_value("washing_machine", True)
            item_loader.add_value("dishwasher", True)
        
        parking=response.xpath("//div[@class='c']/ul/li/span[contains(.,'Otopark')]/text()").extract()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace=response.xpath("//div[@class='title']/h1/span[contains(., 'TERAS')]/text()").extract()
        if terrace:
            item_loader.add_value("terrace", True)
        
        swimming_pool=response.xpath("//span[@id='content_ctlRealtyDescription1_lblRealtyDescription'][contains(., 'YÜZME HAVUZU')]//text()").extract()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'center: [')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('center: [')[1].split(',')[0]
            longitude = latitude_longitude.split('center: [')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        name=response.xpath("//div[@class='c']/span[@class='contact-name']/text()").extract_first()
        if name:
            item_loader.add_value("landlord_name", name)
            
        phone=response.xpath("//div[@class='c']/ul/li[2]/span[2]/text()").extract_first()
        if phone:
            item_loader.add_value("landlord_phone",phone)
         
        yield item_loader.load_item()