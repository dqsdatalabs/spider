import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import math

class giuglerimmobiliare_PySpider_canadaSpider(scrapy.Spider):
    name = 'giuglerimmobiliare_net'
    allowed_domains = ['giuglerimmobiliare.net']
    start_urls = [
        'https://www.giuglerimmobiliare.net/properties/casagaby/moderno-monolocale-valle-di-gressoney/',
        'https://www.giuglerimmobiliare.net/properties/lo-smeraldo/principesco-trilocale/',
        'https://www.giuglerimmobiliare.net/properties/walser/nuovo-bilocale-con-giardino-valle-di-gressoney-2/',
        'https://www.giuglerimmobiliare.net/properties/montagna/trilocale-sulle-piste-da-sci-a-weissmatten/',
        'https://www.giuglerimmobiliare.net/properties/saint-jean/nido-per-le-vostre-vacanze/'
    ]
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        item_loader = ListingLoader(response=response)
        external_id = response.css("#property_stats_groups > div:nth-child(1) > ul > li.propriet_codice.wpp_stat_plain_list_codice > span.value::text").get()
        title = response.css("body > main > div.secondary-header.sh__page > div.container > h1::text").get()
        description = response.css("div > div > div.wpp_the_content > p").extract()
        city = response.css("#property_stats_groups > div:nth-child(2) > ul > li.propriet_citt.wpp_stat_plain_list_citt.alt > span.value::text").get()
        address = response.css("#property_stats_groups > div:nth-child(2) > ul > li.propriet_location.wpp_stat_plain_list_location > span.value::text").get() + "," + city
        property_type_info = response.css("#property_stats_groups > div:nth-child(7) > ul > li.propriet_4atipologia_unit_immobiliare.wpp_stat_plain_list_4atipologia_unit_immobiliare > span.value::text").get()
        if property_type_info == "Trilocale" or "Monolocale" or "Bilocale":
            property_type = 'apartment'
        else:
            property_type = 'house'
        square_meters = int(response.css("#property_stats_groups > div:nth-child(10) > ul > li > span.value::text").get())
        room_count = None
        try:
            room_count = int(response.css("#property_stats_groups > div:nth-child(17) > ul > li.propriet_15a_numero_camere_da_letto.wpp_stat_plain_list_15a_numero_camere_da_letto > span.value::text").get())
        except:
            pass
        if room_count is None:
            room_count = 1
        try:
            bathroom_count = int(response.css("#property_stats_groups > div:nth-child(18) > ul > li > span.value::text").get())
        except:
            bathroom_count = int(response.css("#property_stats_groups > div:nth-child(17) > ul > li > span.value::text").get())                                
            pass
        date = response.css("#property_stats_groups > div:nth-child(1) > ul > li.propriet_area.wpp_stat_plain_list_area.alt > span.value::text").get()
        available_date = "2021-"+date.split('/')[1]+'-'+date.split('/')[0]
        images = response.css(".size-large").extract()
        for i in range(len(images)):
            images[i] = images[i].split('srcset=\"')[1].split(' ')[0]
        floor_plan_images = response.css("#gallery-1 > dl > dt > a::attr(href)").get()
        external_images_count = int(len(images))
        rent = response.css("#property_stats_groups > div:nth-child(1) > ul > li.propriet_price.wpp_stat_plain_list_price > span.value::text").get().split('€')[-2].split(' ')[-2]
        rent = int(rent.replace('.',''))
        currency = 'EUR'
        pets_allowed = response.css("#property_stats_groups > div:nth-child(13) > ul > li > span.value::text").get()
        if 'Non' in pets_allowed:
            pets_allowed = False
        else:
            pets_allowed = True
        furnished = response.css("#property_stats_groups > div:nth-child(12) > ul > li.propriet_ammobiliato.wpp_stat_plain_list_ammobiliato.alt > span.value::text").get()
        if "Si" in furnished:
            furnished = True
        else:
            furnished = False
        floor = response.css("#property_stats_groups > div:nth-child(7) > ul > li.propriet_piano_.wpp_stat_plain_list_piano_ > span.value::text").get()
        if 'Terra' in floor:
            floor = '1'
        else:
            floor = floor[0]
        parking = None
        try:
            parking = response.css("#property_stats_groups > div:nth-child(19) > ul > li > span.value::text").get().replace('\u00a0','')
            if parking == "":
                parking = response.css("#property_stats_groups > div:nth-child(18) > ul > li.propriet_box.wpp_stat_plain_list_box > span.value::text").get()
            if "No" in parking:    
                parking = False       
            else:
                parking = True
        except:
            pass
        elevator = response.css("#property_stats_groups > div:nth-child(5) > ul > li > span.value::text").get()
        if "No" in elevator:     
            elevator = False
        else:
            elevator = True
        balcony = None
        terrace = None
        washing_machine = None
        dishwasher = None
        try:
            info_b = response.css("#property_stats_groups > div:nth-child(11) > ul > li.propriet_balconi.wpp_stat_plain_list_balconi.alt > span.value::text").get()
            if info_b is not None:
                balcony = True
            else:
                balcony = False
        except:
            pass
        try:
            info_t = response.css("#property_stats_groups > div:nth-child(11) > ul > li.propriet_terrazzi.wpp_stat_plain_list_terrazzi.alt > span.value::text").get()
            if info_t is not None:
                terrace = True
            else:
                terrace = False
        except:
            pass
        try:
            info_w = response.css("#property_stats_groups > div:nth-child(16) > ul > li.propriet_lavatrice.wpp_stat_plain_list_lavatrice > span.value::text").get()
            if "Si" in info_w:
                washing_machine = True
            else:
                washing_machine = False
        except:
            pass
        try:
            info_d = response.css("#property_stats_groups > div:nth-child(16) > ul > li.propriet_lavastoviglie.wpp_stat_plain_list_lavastoviglie.alt > span.value::text").get()
            if "Si" in info_d:
                dishwasher = True
            else:
                dishwasher = False
        except:
            pass

        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('city',city)
        item_loader.add_value('address', address)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(int(square_meters)*10.764))
        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)
        item_loader.add_value('available_date',available_date)
        item_loader.add_value('images',images)
        item_loader.add_value('floor_plan_images',floor_plan_images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency',currency)
        item_loader.add_value('pets_allowed',pets_allowed)
        item_loader.add_value('furnished',furnished)
        item_loader.add_value('floor',floor)
        item_loader.add_value('parking',parking)
        item_loader.add_value('elevator',elevator)
        item_loader.add_value('balcony', balcony)
        item_loader.add_value('terrace',terrace)
        item_loader.add_value('washing_machine',washing_machine)
        item_loader.add_value('dishwasher',dishwasher)
        item_loader.add_value('landlord_name','giuglerimmobiliare')
        item_loader.add_value('landlord_email','giuglerimmobiliare@gmail.com')
        item_loader.add_value('landlord_phone','+39-3356100743')
        
        yield item_loader.load_item()


