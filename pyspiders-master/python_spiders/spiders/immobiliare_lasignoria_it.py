# -*- coding: utf-8 -*-
# Author: Noor



import scrapy
from ..loaders import ListingLoader

class MySpider(scrapy.Spider):
    name = 'immobiliare_lasignoria_it'
    allowed_domains = ['immobiliare-lasignoria.it']
    start_urls = ['https://www.immobiliare-lasignoria.it/0063/mn/it/default.asp?tp=affitto']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source="{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'
    pages_links = []


    def parse(self, response, *args):
        self.pages_links = response.css('.link_ricerca').xpath('@href').extract()
        self.pages_links = list(dict.fromkeys(self.pages_links))
        for lnk in self.pages_links:
                link ='https://www.immobiliare-lasignoria.it/0063/mn/it/'+lnk
                yield scrapy.Request(url=link, callback=self.get_property_details,cb_kwargs={'link': link}, dont_filter=True)

    def get_property_details(self, response,link):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', link)
        analyzed_text = response.css('.tab_dati').extract()[0]

        title=response.css('.txt_titoloboxricerca .txt_titolorif::text').extract()[0]
        item_loader.add_value('title',title)

        address_index = analyzed_text.find('Zona:</strong>') + len('Zona:</strong>')
        address = analyzed_text[address_index:analyzed_text.find('<br>', address_index)].strip()
        item_loader.add_value('address', address)

        item_loader.add_value('city',address[address.rfind('-')+1:])


        sq_meters_index = analyzed_text.find('Superficie mq.:</strong>') + len('Superficie mq.:</strong>')
        sq_meters = int(analyzed_text[sq_meters_index:analyzed_text.find('<br>', sq_meters_index)].strip())
        item_loader.add_value('square_meters', sq_meters)

        elevator_index = analyzed_text.find('Ascensore:</strong>') + len('Ascensore:</strong>')
        is_there_elevator = analyzed_text[elevator_index:analyzed_text.find('<br>', elevator_index)].strip()
        if "NO" in is_there_elevator:
            item_loader.add_value('elevator', False)
        elif "SI" in is_there_elevator:
            item_loader.add_value('elevator', True)
        if 'Ascensore:</strong>' not in analyzed_text:
            item_loader.add_value('elevator',False)

        external_id=response.css('.txt_titoloboxricerca .txt_titolorif::text').extract()[0][13:19]
        item_loader.add_value('external_id', external_id)

        rent_string = response.css('.txt_titolorif > *:nth-child(1)::text').extract()[0]
        item_loader.add_value('rent_string', rent_string)



        property_type=''
        if(external_id[0]=='1'):
            property_type='house'
        elif (external_id[0] == '4'):
            property_type = 'apartment'
        elif (external_id[0] == '6'):
            property_type = 'garage'

        item_loader.add_value('property_type',property_type )

        description_index = analyzed_text.find('Descrizione:</strong>') + len('Descrizione:</strong>')
        description = analyzed_text[description_index:analyzed_text.find('<br>', description_index)].strip()
        item_loader.add_value('description', description)


        floor_index = analyzed_text.find('Piano:</strong>') + len('Piano:</strong>')
        floor = analyzed_text[floor_index:analyzed_text.find('<br>', floor_index)].strip()
        item_loader.add_value('floor', floor)

        bath_index = analyzed_text.find('Servizi:</strong>') + len('Servizi:</strong>')
        bath = analyzed_text[bath_index:analyzed_text.find('<br>', bath_index)].strip()
        if bath=='1\xa0DOCCIA':
            item_loader.add_value('bathroom_count', 1)
        elif bath=='2\xa0DOCCIA':
            item_loader.add_value('bathroom_count', 2)


        parking_index = analyzed_text.find('Garage/Posto auto:</strong>') + len('Garage/Posto auto:</strong>')
        is_there_parking = analyzed_text[parking_index:analyzed_text.find('<br>', parking_index)].strip()
        if "NO" in is_there_parking:
            item_loader.add_value('parking', False)
        elif "NO" not in is_there_parking and len(is_there_parking)>0:
            item_loader.add_value('parking', True)
        if 'Garage/Posto auto:</strong>' not in analyzed_text:
            item_loader.add_value('parking',False)

        images =[]
        im=response.css('.txt_titoloboxricerca::text').extract()
        if im:
            imgs_number = int(im[0][2:])
            main_img = response.css('img').xpath('@src').extract()[2]
            images.append(main_img)
            for i in range(2,imgs_number+1):
                if i < 10:
                    img = main_img[:-5] + str(i) + main_img[len(main_img) - 4:]
                else:
                    img = main_img[:-6] + str(i) + main_img[len(main_img) - 4:]
                images.append(img)
            item_loader.add_value('images', images)

        room_count_index=analyzed_text.find('Numero vani:</strong>')+len('Numero vani:</strong>')
        room_count=int(analyzed_text[room_count_index:analyzed_text.find('<br>',room_count_index)].strip())
        item_loader.add_value('room_count',room_count)

        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'immobiliare-lasignoria')
        item_loader.add_value('landlord_email', 'info@immobiliare-lasignoria.it')
        item_loader.add_value('landlord_phone', ' 0556236119')
        item_loader.add_value('external_source', self.external_source)
        if property_type !='garage':
            yield item_loader.load_item()