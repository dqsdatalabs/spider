# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = 'lucasfox_com'
    execution_type='testing'
    country='spain'
    locale='es'
    external_source='Lucasfox_PySpider_spain_es'
    custom_settings={
        # "PROXY_FR_ON": True,
        "HTTPCACHE_ENABLED": False,
        # "COOKIES_ENABLED":False,

    }
    custom_settings = {
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307],
        # "PROXY_ON": True,
        "HTTPCACHE_ENABLED": True
    }
    

    def start_requests(self):
        headers = {

                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-encoding": "gzip, deflate, br",
                # "accept-language": "tr,en;q=0.9,la;q=0.8",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.105 YaBrowser/21.3.3.230 Yowser/2.5 Safari/537.36",
                # "sec-fetch-dest": "document",
                # "sec-fetch-mode": "navigate",
                # "sec-fetch-site": "same-origin",
                # "sec-fetch-user": "?1",
                # "upgrade-insecure-requests": "1",
            }

        start_urls = [
            {
                "url" : [
                    "https://www.lucasfox.es/buscar.html?classid=res_rent&typeid=74B1920785",
                    "https://www.lucasfox.es/buscar.html?classid=res_rent&typeid=88D6CB38FF",
                    # "https://www.lucasfox.com/search.html?classid=res_rent&typeid=88D6CB38FF"
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.lucasfox.es/buscar.html?classid=res_rent&typeid=0053BE9BFF",
                    "https://www.lucasfox.es/buscar.html?classid=res_rent&typeid=A074A6CF72",
                    "https://www.lucasfox.es/buscar.html?classid=res_rent&typeid=57D53735E2",
                    "https://www.lucasfox.es/buscar.html?classid=res_rent&typeid=88D6CB38FF",
                    "https://www.lucasfox.es/buscar.html?classid=res_rent&typeid=6B34D377FF"

                ],
                "property_type" : "house"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            # headers=headers,
                            callback=self.parse,
                           
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//p[@class='c-property-card__title']//a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})


        next_page = response.xpath("//div[@class='c-listing-pagination']//a[@aria-label='Siguiente »']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//span[@class='sash avail_res_rent']//text()[contains(.,'Alquilado')]").get()
        if status:
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "//div[@class='c-detail-showcase__overview__main']//span[@class='c-detail-showcase-titles__ref']/text()")

        item_loader.add_value("external_source", "Lucasfox_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div[@class='c-detail-showcase__overview__main']//span[@class='c-detail-showcase-titles__type']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        bathroom_count = response.xpath("//div[@class='c-detail-showcase__overview__main']//li[span[.='Baños']]/span[@class='c-key-facts__value']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        description = " ".join(response.xpath("//div[@class='c-detail-body__intro']//p/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        features = " ".join(response.xpath("//div[@class='c-detail-body__intro']//p//text()").getall())
        if features:
            if 'se aceptan mascotas' in features.lower():
                item_loader.add_value("pets_allowed", True)
            if 'unfurnished' in features.lower():
                item_loader.add_value("furnished", False)
            if 'amueblada' in features.lower():
                item_loader.add_value("furnished", True)
            if 'elevator' in features.lower():
                item_loader.add_value("elevator", True)
            if 'lavadero' in features.lower():
                item_loader.add_value("washing_machine", True)
            if 'dishwasher' in features.lower():
                item_loader.add_value("dishwasher", True)
            if "piscina" in features.lower():
                item_loader.add_value("swimming_pool", True)
            if "terraza" in features.lower():
                item_loader.add_value("terrace", True)
            if "garaje" in features.lower() or "parqué" in features.lower():
                item_loader.add_value("parking", True)
            if "balcón" in features.lower():
                item_loader.add_value("balcony", True)

        available_date = response.xpath("//text()[contains(.,'Available from')]").get()
        if available_date:
            available_date = " ".join(available_date.split('Available from')[-1].strip().split(' ')[0:3]).strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        address = " ".join(response.xpath("//ul[@class='c-crumb']/li//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        latitude_longitude = response.xpath("//script[contains(.,'hasMap')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('/maps/@')[1].split(',')[0]
            longitude = latitude_longitude.split('/maps/@')[1].split(',')[1].split(',')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        zipcode = response.xpath("//script[contains(.,'postalCode')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split('postalCode":')[1].split(',')[0].strip().strip('"'))

        square_meters = response.xpath("//div[@class='c-detail-showcase__overview__main']//li[span[.='Dimensiones']]/span[@class='c-key-facts__value']/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)
        else:
            sq = response.xpath("//div[@class='c-detail-showcase__overview__main']//li/span[contains(.,'parcela')]//following-sibling::span/text()").get()
            if sq:
                sq = sq.split('m')[0].strip()
                item_loader.add_value("square_meters", sq)
       
        room_count = response.xpath("//div[@class='c-detail-showcase__overview__main']//li[span[.='Dormitorios']]/span[@class='c-key-facts__value']/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        rent = response.xpath("//div[@class='c-detail-showcase__overview__main']//div[@class='c-detail-showcase-titles__price']/text()").extract_first()
        if "on request" not in rent:
            rent = rent.strip().replace(',', '')
            item_loader.add_value("rent_string", rent)

        currency = 'EUR'
        item_loader.add_value("currency", currency)

        external_id = response.xpath("//span[@class='c-detail-showcase-titles__ref']/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        city = response.xpath("//ul[@class='c-crumb']/li[2]/a/text()").get()
        if city:
            city = city.strip()
            item_loader.add_value("city", city)

        images = [x for x in response.xpath("//li/a[@data-fancybox='imagegallery']/@href | //div[@class='c-detail-body__images']//a/@href").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))
        imagescheck=item_loader.get_output_value("images")
        if not imagescheck:
            images=response.xpath("//figure[@class='c-detail-secondary-image']//img//@src").getall()
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", str(len(images)))


        energy_label = response.xpath("//img[@class='energy-cert']/@alt").get()
        if energy_label:
            energy_label = energy_label.strip().split(' ')[-1]
            item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", "Lucas Fox")
        
        landlord_phone = response.xpath("//div[contains(@class,'detail-cta-form')]//a[contains(@href,'tel')]/text()[2]").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        else: item_loader.add_value("landlord_phone", "(+34) 933 562 989")
        
        landlord_email = response.xpath("//div[contains(@class,'detail-cta-form')]//a[contains(@href,'mailto')]/text()[2]").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        else: item_loader.add_value("landlord_email", "info@lucasfox.com")
        
        yield item_loader.load_item()

