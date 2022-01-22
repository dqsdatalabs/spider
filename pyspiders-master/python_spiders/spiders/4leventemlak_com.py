# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser

class MySpider(Spider):
    name = '4leventemlak_com'
    start_urls = ['http://www.4leventemlak.com/index.php?sayfa=emlak_sonuc&mode=hizli_arama&gm_durumu=2&gm_tipi=0&ob=T&yon=D&k=0']  # LEVEL 1
    execution_type='testing'
    country='turkey'
    locale='tr'

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 10)
        
        seen = False
        for item in response.xpath("//td[@class='top p_7']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 10 or seen:
            url = f"http://www.4leventemlak.com/index.php?sayfa=emlak_sonuc&mode=hizli_arama&gm_durumu=2&gm_tipi=0&ob=T&yon=D&k={page}"
            yield Request(url, callback=self.parse, meta={"page": page+10})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath("//td[contains(@class, '5 p_t_10')]/table/tr[1]/td//text()").get()
        if title:
            title = title.strip()
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "4leventemlak_PySpider_"+ self.country + "_" + self.locale)

        address = response.xpath("//td[contains(@class, 'font_10 color_35 p_l_5')]/text()").get()
        if address:
            address = address.strip()
            item_loader.add_value("address", address)


        room_count = response.xpath("//td[contains(.,'Oda Sayısı') and contains(@class, 'p_l_8')]/parent::tr/td[2]/text()").get()
        if room_count:
            if "+" in room_count:
                room_count = str(int(room_count.split('+')[0].strip()) + int(room_count.split('+')[1].strip()))
            elif "Stüdyo" in room_count:
                room_count = "1"
            else:
                room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom=response.xpath("//td[contains(.,'Banyo') and contains(@class, 'p_l_8')]/parent::tr/td[2]/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        property_type = ''
        status = False
        apartment_list = ['DAİRE', 'BİNA', 'DUBLEX', 'REZİDANS', 'ARAKAT', 'APART']
        if not status:
            for element in apartment_list:
                if element in title:
                    property_type = 'apartment'
                    status = True
                    break
        if not status:
            if room_count:
                if int(room_count) > 1:
                    property_type = 'apartment'
                else:
                    property_type = 'apartment'
        if property_type:
            item_loader.add_value("property_type", property_type)
        else: return

        square_meters = response.xpath("//td[contains(.,'Metrekare') and contains(@class, 'p_l_8')]/parent::tr/td[2]/text()").get()
        if square_meters:
            square_meters = square_meters.strip()
            item_loader.add_value("square_meters", square_meters)  

        rent = response.xpath("normalize-space(//strong[@class='color_33 font_14']/text())").get()
        if rent:
            rent = rent.strip().replace(" ","")
            item_loader.add_value("rent_string", rent)

        external_id = response.xpath("//td[contains(.,'İlan No') and contains(@class, 'p_l_8')]/parent::tr/td[2]/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//td[@class='p_5 font_12 color_39 top']//text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if "otopark" in desc_html.lower():
            item_loader.add_value("parking", True)
        if "teras" in desc_html.lower():
            item_loader.add_value("terrace", True)
            
        elevator=response.xpath(
            "//td[@class='p_5 font_12 color_39 top']//text()[contains(.,'Asansör') or contains(.,'asansör')]"
            ).get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        city = response.xpath("//td[contains(.,'Şehir') and contains(@class, 'p_l_8')]/parent::tr/td[2]/text()").get()
        if city:
            city = city.strip()
            item_loader.add_value("city", city)

        images = []
        status = 1
        for x in response.xpath("//td[@class='profil_resim border_01 middle center']/parent::tr/parent::table/tr[3]/td/table//img/@src").getall():
            if status == 1:
                images.append(x)
            status *= -1
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))

        floor = response.xpath("//td[contains(.,'Dairenin Katı') and contains(@class, 'p_l_8')]/parent::tr/td[2]/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        landlord_name = response.xpath("//td[@class='color_08 font_10 bold']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//td[@class='p_l_5 color_18 font_10' and contains(.,'+90')]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data