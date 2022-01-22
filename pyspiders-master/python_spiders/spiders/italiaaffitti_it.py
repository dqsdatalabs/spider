# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from re import U
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json 
import re

class MySpider(Spider):
    name = 'italiaaffitti_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Italiaaffitti_PySpider_italy"
    post_url = ["https://www.italiaaffitti.it/wp-admin/admin-ajax.php"]
    
    payload = "action=ecsload&query=%7B%22post_type%22%3A%22immobili%22%2C%22error%22%3A%22%22%2C%22m%22%3A%22%22%2C%22p%22%3A0%2C%22post_parent%22%3A%22%22%2C%22subpost%22%3A%22%22%2C%22subpost_id%22%3A%22%22%2C%22attachment%22%3A%22%22%2C%22attachment_id%22%3A0%2C%22name%22%3A%22%22%2C%22pagename%22%3A%22%22%2C%22page_id%22%3A0%2C%22second%22%3A%22%22%2C%22minute%22%3A%22%22%2C%22hour%22%3A%22%22%2C%22day%22%3A0%2C%22monthnum%22%3A0%2C%22year%22%3A0%2C%22w%22%3A0%2C%22category_name%22%3A%22%22%2C%22tag%22%3A%22%22%2C%22cat%22%3A%22%22%2C%22tag_id%22%3A%22%22%2C%22author%22%3A%22%22%2C%22author_name%22%3A%22%22%2C%22feed%22%3A%22%22%2C%22tb%22%3A%22%22%2C%22paged%22%3A1%2C%22meta_key%22%3A%22%22%2C%22meta_value%22%3A%22%22%2C%22preview%22%3A%22%22%2C%22s%22%3A%22%22%2C%22sentence%22%3A%22%22%2C%22title%22%3A%22%22%2C%22fields%22%3A%22%22%2C%22menu_order%22%3A%22%22%2C%22embed%22%3A%22%22%2C%22category__in%22%3A%5B%5D%2C%22category__not_in%22%3A%5B%5D%2C%22category__and%22%3A%5B%5D%2C%22post__in%22%3A%5B145055%2C115259%2C115274%2C130613%2C115238%2C122044%2C127810%2C112674%2C112684%2C112653%2C131027%2C107745%2C107613%2C107599%2C107507%2C144266%2C107377%2C139732%2C143859%2C107230%2C107136%2C107093%2C107114%2C108777%2C133035%2C121959%2C107037%2C107053%2C125673%2C107021%2C110312%2C110310%2C110270%2C110285%2C136720%2C110264%2C110266%2C110268%2C110258%2C110260%2C110249%2C110251%2C110253%2C110239%2C110243%2C110245%2C110211%2C110226%2C110228%2C110201%2C110189%2C110191%2C110181%2C110185%2C110187%2C145206%2C112526%2C145186%2C112519%2C144920%2C144925%2C112456%2C112420%2C112438%2C112349%2C112370%2C112282%2C112232%2C112243%2C112163%2C112195%2C112210%2C112117%2C122633%2C112029%2C111940%2C111972%2C111905%2C111742%2C111584%2C111520%2C111537%2C111509%2C111432%2C111318%2C23619%2C111192%2C110655%2C45596%2C95447%2C33276%2C144349%5D%2C%22post__not_in%22%3A%5B%5D%2C%22post_name__in%22%3A%5B%5D%2C%22tag__in%22%3A%5B%5D%2C%22tag__not_in%22%3A%5B%5D%2C%22tag__and%22%3A%5B%5D%2C%22tag_slug__in%22%3A%5B%5D%2C%22tag_slug__and%22%3A%5B%5D%2C%22post_parent__in%22%3A%5B%5D%2C%22post_parent__not_in%22%3A%5B%5D%2C%22author__in%22%3A%5B%5D%2C%22author__not_in%22%3A%5B%5D%2C%22facetwp%22%3Atrue%2C%22posts_per_page%22%3A60%2C%22ignore_sticky_posts%22%3Afalse%2C%22suppress_filters%22%3Afalse%2C%22cache_results%22%3Atrue%2C%22update_post_term_cache%22%3Atrue%2C%22lazy_load_term_meta%22%3Atrue%2C%22update_post_meta_cache%22%3Atrue%2C%22nopaging%22%3Afalse%2C%22comments_per_page%22%3A%2250%22%2C%22no_found_rows%22%3Afalse%2C%22order%22%3A%22DESC%22%7D&ecs_ajax_settings=%7B%22current_page%22%3A{}%2C%22max_num_pages%22%3A%225%22%2C%22load_method%22%3A%22loadmore%22%2C%22widget_id%22%3A%2210386d6%22%2C%22post_id%22%3A145055%2C%22theme_id%22%3A214%2C%22change_url%22%3Afalse%2C%22reinit_js%22%3Afalse%7D"
    
    headers = {
        'accept': '*/*',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'accept-language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
    }
    
    def start_requests(self):
        yield Request(
            url=self.post_url[0],
            body = self.payload.format(0),
            headers=self.headers,
            method="POST",
            callback=self.parse,
            meta={
                "base_payload": self.payload
            }
        )
    
    
    
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//a[contains(.,'Visualizza')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 1 or seen:
            payload = response.meta.get('base_payload').format(page)
            yield Request(
                url=self.post_url[0], 
                dont_filter=True,
                body=payload,
                method="POST",
                headers=self.headers,
                callback=self.parse, 
                meta={
                    "page": page+1,
                    "base_payload": response.meta.get('base_payload')
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//div[contains(@class,'element-01960a9')]/following-sibling::div//p/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        dontallow=response.url
        if dontallow and "vendita" in dontallow:
            return 
        adres=response.xpath("//main[@class='site-main clr']//span[contains(.,'Via')]/text()").get()
        if adres:
            item_loader.add_value("address",adres)

        city=response.xpath("//span[@class='elementor-icon-list-text']/text()").get()
        if city:
            item_loader.add_value("city",city.split(",")[-1].strip().split(" ")[-1])

        zipcode=response.xpath("//span[@class='elementor-icon-list-text']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(",")[-1].strip().split(" ")[0])
        utilities=response.xpath("//section[contains(.,'Spese condominiali')]//p[contains(.,'mese')]/text()").get()
        if utilities:
            utilities=re.findall("\d+",utilities)
            if utilities:
                item_loader.add_value("utilities",utilities)


        external_id = response.xpath(
            "//div[@class='elementor-widget-container']//p[@class='elementor-heading-title elementor-size-default'][1]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title) 
        dontallow=response.url
        if dontallow and "vendesi" in dontallow:
            return 

        description = response.xpath(
            "//div[@id='descrizione']//div[@class='elementor-widget-container']//following-sibling::p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//h2[@class='elementor-heading-title elementor-size-default'][1]//text()[contains(.,'€')]").get()
        if rent:
            price= rent.replace(" ",".").split("€")[1].split("/")[0]
            item_loader.add_value(
                "rent", price)
        item_loader.add_value("currency", "EUR")

        address = response.xpath(
            "//span[contains(@class,'elementor-icon-list-text')]//text()").get()
        if address and not "," in address:
            item_loader.add_value("address", address)
            city = ''.join(address.split(' ')[-1:]).strip()
            if not "" in city:
                item_loader.add_value("city", city)
            zipcode = ''.join(address.split(' ')[:-1]).strip()
            if not "" in zipcode:
                item_loader.add_value("zipcode", zipcode.replace(",",""))
        addrescheck=item_loader.get_output_value("address")
        if not addrescheck:
            adres=response.xpath("//h1[@class='elementor-image-box-title']/text()").get()
            if adres:
                item_loader.add_value("address",adres.lower().split("rif")[0].strip().split(" ")[-1].capitalize())
        room_count=response.xpath("//p[.='locali']/parent::div/h3/span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//p[.='bagni']/parent::div/h3/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        furnished=response.xpath("//h3[.='Arredato:']/parent::div/parent::div/parent::div/parent::div/parent::div/following-sibling::div/div/div/div/div/p/text()").get()
        if furnished and "Y" in furnished:
            item_loader.add_value("furnished",True)


        square_meters = response.xpath(
            "//div[@class='elementor-widget-container']//p[@class='elementor-heading-title elementor-size-default'][1]//text()[contains(.,'mq')]").get()
        if square_meters:
            square_meters = square_meters.split("mq")[0]
            item_loader.add_value("square_meters", square_meters)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='elementor-widget-container']//div//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_phone", "0854458724")
        item_loader.add_value("landlord_email", "info@italiaaffitti.it")
        item_loader.add_value("landlord_name", "ITALIA AFFITTI")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartamenti" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("casa" in p_type_string.lower() or "villetta" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None