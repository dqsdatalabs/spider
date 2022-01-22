import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json

class tirrenialivingimmobiliare_it_PySpider_italySpider(scrapy.Spider):
    name = 'tirrenialivingimmobiliare_it'
    allowed_domains = ['tirrenialivingimmobiliare.it']
    page_number = 2
    start_urls = [
        'https://www.tirrenialivingimmobiliare.it/it/immobili?categoriax=1&contratto%5B0%5D=2&contratto%5B1%5D=4&contratto%5B2%5D=5&tipologia%5B0%5D=1&provincia=&prezzo_min=&prezzo_max=&mq_min=&mq_max=&rif=&page=1'
        ]
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


    def parse(self, response):  
        urls = response.css("#proprieta-filtrate > div > a::attr(href)").extract()
        for i in range(len(urls)):
            urls[i] = "https://www.tirrenialivingimmobiliare.it" + urls[i]
        rent_all = response.css("#proprieta-filtrate > div > a > div.listing-img-content > span::text").extract()
        area = response.css("#proprieta-filtrate > div > div > ul > li:nth-child(1) > span::text").extract()
        room = response.css("#proprieta-filtrate > div > div > ul > li:nth-child(2) > span::text").extract()
        bath = response.css("#proprieta-filtrate > div > div > ul > li:nth-child(3) > span::text").extract()
        extenal_id_all = response.css("#proprieta-filtrate > div > div > div.listing-footer > span::text").extract()
        title_all = response.css("#proprieta-filtrate > div > div > div.listing-title > a::attr(title)").extract()
        title_all_edited = []
        for i in range(len(title_all)):
            if "Appartamento in affitto" in title_all[i]:
                title_all_edited.append(title_all[i])
        rent_type = response.css("#proprieta-filtrate > div > a > div.listing-badges > span::text").extract()
        for i in range(len(urls)):
            if rent_type[i] == 'Affitto':
                title = title_all_edited[i]
                rent = int(rent_all[i].split("€")[1])
                city = title_all_edited[i].split("a ")[1]
                square_meters = int(area[i].replace("m",""))
                room_count = room[i]
                bathroom_count = int(bath[i])
                external_id = extenal_id_all[i].split("Rif: ")[1]
                property_type = 'apartment'
                yield Request(url=urls[i],
                callback = self.parse_property,
                meta={
                    'title':title,
                    'rent':rent,
                    'city':city,
                    'square_meters':square_meters,
                    'room_count':room_count,
                    'bathroom_count':bathroom_count,
                    'external_id':external_id,
                    'property_type':property_type
                })
                
        next_page = ("https://www.tirrenialivingimmobiliare.it/it/immobili?categoriax=1&contratto%5B0%5D=2&contratto%5B1%5D=4&contratto%5B2%5D=5&tipologia%5B0%5D=1&provincia=&prezzo_min=&prezzo_max=&mq_min=&mq_max=&rif=&page="+ str(tirrenialivingimmobiliare_it_PySpider_italySpider.page_number))
        if tirrenialivingimmobiliare_it_PySpider_italySpider.page_number <= 3:
            tirrenialivingimmobiliare_it_PySpider_italySpider.page_number += 1
            yield response.follow(next_page, callback=self.parse)

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        
        title = response.meta.get("title")
        rent = response.meta.get("rent")
        city = response.meta.get("city")
        square_meters = response.meta.get("square_meters")
        room_count = response.meta.get("room_count")
        bathroom_count = response.meta.get("bathroom_count")
        external_id = response.meta.get("external_id")
        property_type = response.meta.get("property_type")
        if "n.d." in room_count:
            room_count = 1
        else:
            room_count = int(room_count)
        
        description = response.css("#detail-info > div > div:nth-child(2) > div > div::text").extract()
        
        images = response.css(".fit-contain-center::attr(src)").extract()
        external_images_count = len(images)
        address = response.css(".listing-address").get()
        details = response.css("#detail-info > div > div.property-description.post-content.desc-headline > ul > li::text").extract()
        details_value = response.css("#detail-info > div > div.property-description.post-content.desc-headline > ul > li> span::text").extract()
        furnished = None
        floor = None
        parking = None
        elevator = None
        balcony = None
        for i in range(len(details)):
            if "Arredato: " in details[i]:
                furnished = details_value[i]
                if furnished == 'arredato':
                    furnished = True
                else:
                    furnished = False
            if "Piano: " in details[i]:
                floor = details_value[i]
            if "Posti auto: " in details[i]:
                parking = True
            if "Box auto: " in details[i]:
                if "sì" in details_value[i]:
                    parking = True
                else:
                    parking = False
            if "Ascensore: " in details[i]:
                if "sì" in details_value[i]:
                    elevator = True
                else:
                    elevator = False
            if "Balconi: " in details[i]:
                if "sì" in details_value[i]:
                    balcony = True
                else:
                    balcony = False


        item_loader.add_value('external_link', response.url) 
        item_loader.add_value('external_id',external_id)       
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('title',title)
        item_loader.add_value('description',description)
        item_loader.add_value('address',address)
        item_loader.add_value('city',city)
        item_loader.add_value('property_type',property_type)
        item_loader.add_value('square_meters',square_meters)
        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency','EUR')
        item_loader.add_value('furnished',furnished)
        item_loader.add_value('floor',floor)
        item_loader.add_value('parking',parking)
        item_loader.add_value('elevator',elevator)
        item_loader.add_value('balcony',balcony)
        item_loader.add_value('landlord_name','tirrenialivingimmobiliare')
        item_loader.add_value('landlord_phone','+39 3455854223')
        item_loader.add_value('landlord_email','tirrenialiving.immobiliare@gmail.com')
        yield item_loader.load_item()
