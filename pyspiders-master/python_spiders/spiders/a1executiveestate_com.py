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
import dateparser

class MySpider(Spider):
    name = 'a1executiveestate_com' 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en'
    external_source="A1executiveestate_PySpider_netherlands"
    start_urls = ["https://www.a1executiveestate.com/archive/huur/"] #LEVEL-1

    # 1. FOLLOWING
    def start_requests(self):
        url =  "https://a1executiveestate.com/"
        yield Request(url,callback=self.parse)
 
    def parse(self,response):
        
        formdata = {
            'action': 'us_ajax_grid',
            'ajax_url': 'https://a1executiveestate.com/wp-admin/admin-ajax.php',
            'infinite_scroll': 'true',
            'max_num_pages': '3',
            'pagination': 'ajax',
            'permalink_url': 'https://a1executiveestate.com/woning',
            'template_vars': '{"columns":"3","exclude_items":"none","img_size":"default","ignore_items_size":false,"items_layout":"1676","items_offset":"1","load_animation":"afb","overriding_link":"post","post_id":102,"query_args":{"post_type":["realworks_wonen"],"post_status":["publish","acf-disabled","dp-rewrite-republish"],"post__not_in":[102],"posts_per_page":"9","tax_query":{"relation":"AND"},"meta_query":{"relation":"AND"},"paged":1},"orderby_query_args":{"orderby":{"date":"DESC"}},"type":"grid","us_grid_ajax_index":1,"us_grid_filter_params":"filter_category=for-rent","us_grid_index":1,"_us_grid_post_type":"realworks_wonen"}'
        }
        yield FormRequest(
            "https://a1executiveestate.com/wp-admin/admin-ajax.php",
            formdata=formdata,
            callback= self.parse_list,
            dont_filter=True


        )
    def parse_list(self, response):
        page = response.meta.get('page', 2)
        seen=False

        for url in response.xpath("//div[@class='w-grid-item-h']/a"):
            detail_url = url.xpath("./@href").extract_first()
            yield Request(detail_url,callback=self.populate_item)
            seen = True

        if page == 2 or seen:

            formdata = {
                'action': 'us_ajax_grid',
                'ajax_url': 'https://a1executiveestate.com/wp-admin/admin-ajax.php',
                'infinite_scroll': 'true',
                'max_num_pages': '3',
                'pagination': 'ajax',
                'permalink_url': 'https://a1executiveestate.com/woning',
                'template_vars': '{"columns":"3","exclude_items":"none","img_size":"default","ignore_items_size":false,"items_layout":"1676","items_offset":"1","load_animation":"afb","overriding_link":"post","post_id":102,"query_args":{"post_type":["realworks_wonen"],"post_status":["publish","acf-disabled","dp-rewrite-republish"],"post__not_in":[102],"posts_per_page":"9","tax_query":{"relation":"AND"},"meta_query":{"relation":"AND"},"paged":2},"orderby_query_args":{"orderby":{"date":"DESC"}},"type":"grid","us_grid_ajax_index":1,"us_grid_filter_params":"filter_category=for-rent","us_grid_index":1,"_us_grid_post_type":"realworks_wonen"}'
            } 

            url = "https://a1executiveestate.com/wp-admin/admin-ajax.php"
            yield FormRequest(
                url,
                formdata = formdata,
                method = "POST",
                callback = self.parse_list,
                meta={"page": page+1}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        other = response.xpath("//div[@class='wpb_wrapper']/strong[contains(.,'Other')]/text()").get()
        if other:return

        rented = response.xpath("//div[@class='wpb_wrapper']/span[contains(.,'Rented')]").get()
        if rented:return

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//div[@class='realworks--content']/p/text()").getall())
        # if get_p_type_string(f_text):
        #     item_loader.add_value("property_type", get_p_type_string(f_text))
        # else:
        #     f_text = response.url
        #     if get_p_type_string(f_text):
        item_loader.add_value("property_type", "house")
        #     else:
                # return
        title = response.xpath("//div[@class='wpb_text_column us_custom_2774bef1']/div/text()").extract_first()
        item_loader.add_value("title", title)
        
        address = "".join(response.xpath("//div[@class='wpb_text_column us_custom_2774bef1']/div/text()").getall())
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1])
        
        
        rent = response.xpath("//div[contains(@class,'header-price')]/div/text()").get()
        if rent:
            price = rent.split(",")[0].split("€")[1].strip().replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//span[@id='size']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//span[contains(@id,'bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//span[contains(@id,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[@class='slider single-item']/a[@class='fancybox']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        desc = " ".join(response.xpath("//div[@class='wpb_text_column us_custom_432d3f53']/div/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        furnished = response.xpath("//div[@class='wpb_wrapper']/strong/text()[contains(.,'Furnished')]").get()
        if furnished:
            if "Yes" in furnished:
                item_loader.add_value("furnished", True)

        parking = response.xpath("//div[@class='wpb_wrapper']/strong/text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
      
        floor_plan_images = [x for x in response.xpath("//div[contains(@class,'floorplan')]//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        lat = response.xpath("substring-before(substring-after(//div[@class='placecard__right']/a/@href,'ll='),',')").extract_first()
        if lat :
            item_loader.add_value("latitude", lat)
            item_loader.add_xpath("longitude", "substring-before(substring-after(//div[@class='placecard__right']/a/@href,','),'&z')")
        
        
        balcony = response.xpath("//p//text()[contains(.,'Balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        if "energielabel" in desc.lower():
            energy_label = desc.lower().split("energielabel")[1].strip().split(" ")[0]
            item_loader.add_value("energy_label", energy_label.upper())
        
        if ",- euro service cost" in desc:
            utilities = desc.split(",- euro service cost")[0].split("excl.")[1]
            item_loader.add_value("utilities", utilities)
        elif "service cost" in desc.replace("k","c"):
            utilities = desc.replace("k","c").split("service cost")[1].split(",-")[0].strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)
        elif "cost" in desc.replace("of",""):
            utilities = desc.replace("of","").split("cost")[1].split(",-")[0].strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)        
        
        item_loader.add_value("landlord_name", "A1 Executive Estate")
        item_loader.add_value("landlord_phone", "+31(0) 20 4410088")
        item_loader.add_value("landlord_email", "info@a1ee.nl")


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None
