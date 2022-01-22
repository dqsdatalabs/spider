# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'remaxolusum_com'
    execution_type='testing'
    country='turkey'
    locale='tr'  
    
    def start_requests(self):

        url = "http://remaxolusum.com/portfoy"
        r_estate_status = ""
        payload = f"------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"q_s\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_pro\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_ilantur\"\r\n\r\nKiralık\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_il\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_ilce\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_semt\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_gaytur\"\r\n\r\nKonut\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_emlakdurum\"\r\n\r\n{r_estate_status}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_esyadurum\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_odas\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_kredidurum\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_fiyattur\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_fiyat_min\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_fiyat_max\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_metren_min\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_metren_max\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"q_sub\"\r\n\r\nARA\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"prev\"\r\n\r\n0\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"
        headers = {
            'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW"
            }

        start_payloads = [
            {
                "status" : "Apartman",
                "property_type" : "apartment"
            },
            {
                "status" : "Daire",
                "property_type" : "apartment"
            },
        ]
        for item in start_payloads:
            r_estate_status = item["status"]
            prop_type = item["property_type"]
            yield Request(
                url,
                headers = headers,
                body = payload,
                method = "POST",
                callback = self.parse,
                meta={'property_type': prop_type, "status": r_estate_status})




    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get('page', 12)
        r_estate_status = response.meta.get('status')
        
        seen = False
        for item in response.xpath("//div[@class='col-md-4']/div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get("property_type")})
            seen = True
        
        if page == 12 or seen:
            url = "http://remaxolusum.com/portfoy"
            payload = f"------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"q_s\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_pro\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_ilantur\"\r\n\r\nKiralık\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_il\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_ilce\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_semt\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_gaytur\"\r\n\r\nKonut\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_emlakdurum\"\r\n\r\n{r_estate_status}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_esyadurum\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_odas\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_kredidurum\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_fiyattur\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_fiyat_min\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_fiyat_max\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_metren_min\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"r_metren_max\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"q_sub\"\r\n\r\nARA\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"next\"\r\n\r\n{page}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"
            headers = {
                'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW"
                }
            yield Request(
                url,
                headers = headers,
                body = payload,
                method = "POST",
                callback = self.parse,
                meta={"page": page+12, "status": r_estate_status, "property_type": response.meta.get("property_type")}
                )
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Remaxolusum_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div[contains(@class,'p_detay')]/h1/text()").get()
        item_loader.add_value("title", title)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("external_id", "//ul/li[contains(.,'İlan No')]//following-sibling::span/text()")
        
        rent="".join(response.xpath("//div[@id='in_p']/div[2]//text()").extract())
        if rent:
            item_loader.add_value("rent", rent.split("TL")[0].strip())
            item_loader.add_value("currency", "TRY")
            
        address = " ".join(response.xpath("//div[@id='in_p']/div[1]//text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split('/')[0].strip())

        latitude_longitude = response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        square_meters=response.xpath("//ul/li[contains(.,'Net')]//following-sibling::span/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        
        room_count=response.xpath("//ul/li[contains(.,'Oda')]//following-sibling::span/text()").extract_first()
        if room_count:
            room_count=room_count.split("+")
            item_loader.add_value("room_count", str(int(room_count[0])+int(room_count[1])))
        
        bathroom=response.xpath("//ul/li[contains(.,'Banyo')]//following-sibling::span/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
            
        floor=response.xpath("//ul/li[contains(.,'Bulunduğu Kat')]//following-sibling::span/text()[not(contains(.,'Giriş')) and not(contains(.,'Bahçe Katı'))]").extract_first()
        if floor:
            item_loader.add_value("floor", floor)
            
        furnished=response.xpath("//ul/li[contains(.,'Eşya')]//following-sibling::span/text()[contains(.,'Evet')]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
        utilities=response.xpath("//ul/li[contains(.,'Aidat')]//following-sibling::span/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities)

        item_loader.add_xpath("bathroom_count", "//li[span[.='Banyo Sayısı']]/span[2]/text()")
        desc="".join(response.xpath("//div[contains(@class,'col-md-12 n_ps')]//text()").extract())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))

        images=[x for x in response.xpath("//img[@class='mini']/@src").getall()]
        for i in images:
            item_loader.add_value("images","http://remaxolusum.com/"+i)
        item_loader.add_value("external_images_count", str(len(images)))
        
        swimming_pool=response.xpath(
            "//div[contains(@class,'col-md-12 n_ps')]//text()[contains(.,'HAVUZ') or contains(.,'havuz') or contains(.,'Havuz')]"
        ).get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        furnished=response.xpath("//li[span[.='Eşya Durumu :']]/span[2]/text()").get()
        if furnished:
            if furnished== "EVET":
                item_loader.add_value("swimming_pool", True)
            elif furnished == "HAYIR":
                item_loader.add_value("swimming_pool", False)
        parking=response.xpath(
            "//div[contains(@class,'col-md-12 n_ps')]//text()[contains(.,'OTOPARK') or contains(.,'otopark') or contains(.,'Otopark')]"
        ).get()
        if parking:
            item_loader.add_value("parking", True)


        item_loader.add_xpath("landlord_phone", "substring-after(//div[@class='col-md-12 l_p']/p/text(),'GSM : ')")
        item_loader.add_xpath("landlord_name", "//div[@class='col-md-7']/h3//text()")
        item_loader.add_xpath("landlord_email", "//div[@class='col-md-12 l_p']/p/text()[contains(.,'@')]")
        
        yield item_loader.load_item()
        